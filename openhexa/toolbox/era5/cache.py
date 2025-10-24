import gzip
import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg.rows import class_row

from openhexa.toolbox.era5.models import Request


@dataclass
class CacheEntry:
    job_id: str
    file_name: str | None


def _hash_data_request(request: Request) -> str:
    """Convert data request dict into MD5 hash."""
    json_str = json.dumps(request, sort_keys=True)
    return hashlib.md5(json_str.encode()).hexdigest()


class Cache:
    """Cache data requests using PostgreSQL."""

    def __init__(self, database_uri: str, cache_dir: Path):
        """Initialize cache in database.

        Args:
            database_uri: URI of the PostgreSQL database, e.g.
                "postgresql://user:password@host:port/dbname".
            cache_dir: Directory to store downloaded GRIB files.
        """
        self.database_uri = database_uri
        self.cache_dir = cache_dir
        self._init_db()
        self._init_cache_dir()

    def _init_db(self) -> None:
        """Create schema and table if they do not exist."""
        with psycopg.connect(self.database_uri) as conn:
            with conn.cursor() as cur:
                cur.execute("create schema if not exists era5")
                cur.execute(
                    """
                    create table if not exists era5.cds_cache (
                        cache_key varchar(32) primary key,
                        request json not null,
                        job_id varchar(64) not null,
                        file_name text,
                        created_at timestamp not null default now(),
                        updated_at timestamp not null default now(),
                        expires_at timestamp
                    )
                    """
                )
                cur.execute(
                    """
                    create index if not exists idx_cds_cache_expires
                        on era5.cds_cache(expires_at)
                    """
                )
                conn.commit()

    def _init_cache_dir(self) -> None:
        """Create cache directory if it does not exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _archive(self, src_fp: Path) -> None:
        """Archive a GRIB file using gzip.

        Args:
            src_fp: Path to the source GRIB file.
        """
        dst_fp = self.cache_dir / f"{src_fp.name}.gz"
        with open(src_fp, "rb") as src_f:
            with gzip.open(dst_fp, "wb", compresslevel=9) as dst_f:
                shutil.copyfileobj(src_f, dst_f)

    def retrieve(self, job_id: str, dst_fp: Path) -> None:
        """Retrieve a GRIB file from a gzip archive.

        Args:
            job_id: The ID of the corresponding CDS job.
            dst_fp: Path to the destination GRIB file.
        """
        src_fp = self.cache_dir / f"{job_id}.grib.gz"
        if not src_fp.exists():
            raise FileNotFoundError(f"Cached file not found: {src_fp}")
        with gzip.open(src_fp, "rb") as src_f:
            with open(dst_fp, "wb") as dst_f:
                shutil.copyfileobj(src_f, dst_f)

    def set(self, request: Request, job_id: str, file_path: Path | None = None) -> None:
        """Store a data request in the cache.

        Data request info and metadata are stored in the database. If available, the
        downloaded GRIB file is archived in the cache directory.

        Args:
            request: The data request parameters.
            job_id: The ID of the corresponding CDS job.
            file_path: Optional path to the downloaded file to be cached.
        """
        if file_path:
            self._archive(file_path)
            file_name = f"{file_path.name}.gz"
        else:
            file_name = None

        cache_key = _hash_data_request(request)
        with psycopg.connect(self.database_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into era5.cds_cache (
                        cache_key, request, job_id, file_name
                    ) values (%s, %s, %s, %s)
                    on conflict (cache_key) do update set
                        job_id = excluded.job_id,
                        file_name = excluded.file_name,
                        updated_at = now()
                    """,
                    (cache_key, json.dumps(request), job_id, file_name),
                )
                conn.commit()

    def get(self, request: Request) -> CacheEntry | None:
        """Retrieve a data request from the cache.

        Args:
            request: The data request parameters.
        """
        cache_key = _hash_data_request(request)
        with psycopg.connect(self.database_uri) as conn:
            with conn.cursor(row_factory=class_row(CacheEntry)) as cur:
                cur.execute(
                    """
                    select job_id, file_name from era5.cds_cache
                    where cache_key = %s
                    """,
                    (cache_key,),
                )
                return cur.fetchone()

    def clean_expired_jobs(self, job_ids: list[str]) -> None:
        """Remove cache entries associated with expired jobs.

        NB: The entry is only removed if the associated GRIB file has not been archived
        yet.

        Args:
            job_ids: The IDs of the expired CDS jobs.
        """
        with psycopg.connect(self.database_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    delete from era5.cds_cache
                    where job_id = any(%s) and file_name is null
                    """,
                    (job_ids,),
                )
                conn.commit()

    def clean_missing_files(self) -> None:
        """Remove cache entries with missing archived files."""
        with psycopg.connect(self.database_uri) as conn:
            with conn.cursor(row_factory=class_row(CacheEntry)) as cur:
                cur.execute(
                    """
                    select job_id, file_name from era5.cds_cache
                    where file_name is not null
                    """
                )
                entries = cur.fetchall()

            missing_job_ids: list[str] = []
            for entry in entries:
                if entry.file_name and not (self.cache_dir / entry.file_name).exists():
                    missing_job_ids.append(entry.job_id)

            if missing_job_ids:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        delete from era5.cds_cache
                        where job_id = any(%s)
                        """,
                        (missing_job_ids,),
                    )
                    conn.commit()
