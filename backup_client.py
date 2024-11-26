import datetime
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import List, Dict, Awaitable, Optional

import aiofiles
import aiohttp
import yadisk
import yaml
from pydantic import BaseModel, Field
from tenacity import retry, wait_fixed, stop_after_attempt

from common import StringEnum
from logger import get_common_logger

logger = get_common_logger(__name__)


class GitOrigin(StringEnum):
    github = "github"


class ConfigRepo(BaseModel):
    git_origin: GitOrigin = Field(alias="git-origin")
    organization: str
    repository: str
    branch: str
    backups_limit: Optional[int] = Field(alias="backups-limit", default=None)


class Config(BaseModel):
    backups_limit: int = Field(alias="backups-limit")
    repos: List[ConfigRepo]


class BackupClient:
    _CONFIG_PATH = "./config.yml"
    _DOWNLOADED_REPOS_FOLDER_PATH = "./downloaded_repos"
    _READ_ARCHIVE_CHUNK_BYTES = 102400  # 100 KB

    _ROOT_PATH = "app:/"
    _YANDEX_DISK_RETRIES_COUNT = 5
    _YANDEX_DISK_RETRIES_INTERVAL_SECONDS = 1

    def __init__(self,
                 yandex_disk_token: str,
                 github_token: str):
        self._config = self._read_config()
        self._yandex_disk_client = yadisk.AsyncClient(token=yandex_disk_token, session="aiohttp")
        self._github_headers = {"Authorization": f"Bearer {github_token}"}
        self._git_origin_to_download_repo_archive_method: \
            Dict[GitOrigin, Callable[[ConfigRepo, str], Awaitable[None]]] = {
            GitOrigin.github: self._download_github_repo_archive
        }

    async def back_up_repos(self):
        for repo in self._config.repos:
            await self._back_up_repo(repo)
            await self._delete_outdated_repo_backups(repo)

        # Deleting directory with downloaded repositories
        shutil.rmtree(self._DOWNLOADED_REPOS_FOLDER_PATH)

    async def _back_up_repo(self, repo: ConfigRepo):
        logger.info(f"Performing back up for repo={{{repo}}}")
        git_origin_value = repo.git_origin.value
        organization = repo.organization
        repository = repo.repository

        archive_name = f"{git_origin_value}_{organization}_{repository}_{datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.zip"
        archive_folder_path = f"{self._DOWNLOADED_REPOS_FOLDER_PATH}/{git_origin_value}/{organization}/{repository}"
        archive_path = f"{archive_folder_path}/{archive_name}"
        Path(archive_folder_path).mkdir(parents=True, exist_ok=True)

        download_repo_archive_method = self._git_origin_to_download_repo_archive_method[repo.git_origin]
        await download_repo_archive_method(repo, archive_path)
        await self._upload_repo_archive_to_yandex_disk(repo=repo, archive_path=archive_path)

        logger.info(f"Finished back up for repo={{{repo}}}")

    async def shutdown(self):
        await self._yandex_disk_client.close()

    def _read_config(self) -> Config:
        with open(self._CONFIG_PATH, "r") as f:
            raw_config = yaml.safe_load(f)
            return Config.model_validate(raw_config)

    async def _download_github_repo_archive(self, repo: ConfigRepo, archive_path: str):
        async with aiohttp.ClientSession(headers=self._github_headers) as session:
            url = f"https://api.github.com/repos/{repo.organization}/{repo.repository}/zipball/{repo.branch}"
            async with session.get(url) as r:
                r.raise_for_status()
                async with aiofiles.open(archive_path, "wb") as f:
                    while True:
                        chunk = await r.content.read(self._READ_ARCHIVE_CHUNK_BYTES)  # Read in 1KB chunks
                        if not chunk:
                            break
                        await f.write(chunk)
        logger.info(f"Successfully downloaded repo={{{repo}}}")

    @retry(wait=wait_fixed(_YANDEX_DISK_RETRIES_INTERVAL_SECONDS),
           stop=stop_after_attempt(_YANDEX_DISK_RETRIES_COUNT))
    async def _upload_repo_archive_to_yandex_disk(self, repo: ConfigRepo, archive_path: str):
        git_origin_value = repo.git_origin.value

        root_path = "app:/"
        relative_file_folder_path = f"{git_origin_value}/{repo.organization}/{repo.repository}"
        archive_file_name_non_zip = f"{git_origin_value}_{repo.organization}_{repo.repository}_{datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}"
        file_path = f"{root_path}{git_origin_value}/{repo.organization}/{repo.repository}/{archive_file_name_non_zip}"
        await self._prepare_folders_for_path(root_path=root_path,
                                             relative_folder_path=relative_file_folder_path)

        upload_link = await self._yandex_disk_client.get_upload_link(
            file_path,
            overwrite=True,
            n_retries=self._YANDEX_DISK_RETRIES_COUNT,
            retry_interval=self._YANDEX_DISK_RETRIES_INTERVAL_SECONDS
        )

        await self._yandex_disk_client.upload_by_link(
            archive_path,
            upload_link,
            n_retries=self._YANDEX_DISK_RETRIES_COUNT,
            retry_interval=self._YANDEX_DISK_RETRIES_INTERVAL_SECONDS
        )
        # Yandex Disk slows down the download of .zip archives, so we upload as a raw file and then rename it.
        await self._yandex_disk_client.rename(
            file_path,
            f"{archive_file_name_non_zip}.zip",
            n_retries=self._YANDEX_DISK_RETRIES_COUNT,
            retry_interval=self._YANDEX_DISK_RETRIES_INTERVAL_SECONDS
        )

        logger.info(f"Successfully uploaded repo={{{repo}}}")

    async def _prepare_folders_for_path(self, root_path: str, relative_folder_path: str):
        if root_path.endswith("/"):
            root_path = root_path.removesuffix("/")

        folders = relative_folder_path.split("/")
        current_folder_path = ""
        for folder in folders:
            stripped_folder = folder.strip()
            if not stripped_folder:
                continue
            current_folder_path += f"/{stripped_folder}"

            full_folder_path = f"{root_path}{current_folder_path}"
            if not await self._yandex_disk_client.exists(
                    full_folder_path,
                    n_retries=self._YANDEX_DISK_RETRIES_COUNT,
                    retry_interval=self._YANDEX_DISK_RETRIES_INTERVAL_SECONDS
            ):
                logger.info(f"Path {{{full_folder_path}}} doesn't exist. Creating...")
                await self._yandex_disk_client.mkdir(full_folder_path)
                logger.info(f"Path {{{full_folder_path}}} is successfully created.")

    async def _delete_outdated_repo_backups(self, repo: ConfigRepo):
        backups_limit = self._get_repo_backups_limit(repo)
        logger.info(f"Started deleting outdated backups for repo={{{repo}}} with backups-limit={{{backups_limit}}}.")
        backups_count = 0
        backups_to_delete = []
        backups_dir_path = self._get_absolute_backup_dir_path(repo)
        async for backup in self._yandex_disk_client.listdir(
                backups_dir_path,
                sort="-created",
                n_retries=self._YANDEX_DISK_RETRIES_COUNT,
                retry_interval=self._YANDEX_DISK_RETRIES_INTERVAL_SECONDS
        ):
            backups_count += 1
            if backups_count > backups_limit:
                backups_to_delete.append(backup)

        logger.info(f"Found {len(backups_to_delete)} backups for repo={{{repo}}} "
                    f"that exceed the limit on backups.")
        for backup in backups_to_delete:
            logger.info(f"Deleting backup={{{backup.name}}} for repo={{{repo}}}...")
            await self._yandex_disk_client.remove(
                backup.path,
                n_retries=self._YANDEX_DISK_RETRIES_COUNT,
                retry_interval=self._YANDEX_DISK_RETRIES_INTERVAL_SECONDS
            )
            logger.info(f"Deleted backup={{{backup.name}}} for repo={{{repo}}}.")
        logger.info(f"Finished deleting outdated backups for repo={{{repo}}}.")

    def _get_non_zip_backup_file_path(self, repo: ConfigRepo):
        return (f"{self._ROOT_PATH}{self._get_relative_backup_dir_path(repo)}/"
                f"{self._get_non_zip_backup_file_name(repo)}")

    def _get_absolute_backup_dir_path(self, repo: ConfigRepo) -> str:
        return f"{self._ROOT_PATH}{self._get_relative_backup_dir_path(repo)}"

    def _get_repo_backups_limit(self, repo: ConfigRepo) -> int:
        return repo.backups_limit or self._config.backups_limit

    @staticmethod
    def _get_relative_backup_dir_path(repo: ConfigRepo) -> str:
        return f"{repo.git_origin.value}/{repo.organization}/{repo.repository}"

    @staticmethod
    def _get_non_zip_backup_file_name(repo: ConfigRepo) -> str:
        return (f"{repo.git_origin.value}_{repo.organization}_{repo.repository}_"
                f"{datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}")
