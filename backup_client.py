import asyncio
import datetime
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import List, Dict, Awaitable

import aiofiles
import aiohttp
import yadisk
import yaml
from pydantic import BaseModel, Field

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


class Config(BaseModel):
    backups_limit: int = Field(alias="backups-limit")
    repos: List[ConfigRepo]


class BackupClient:
    _CONFIG_PATH = "./config.yml"
    _DOWNLOADED_REPOS_FOLDER_PATH = "./downloaded_repos"
    _READ_ARCHIVE_CHUNK_BYTES = 102400  # 100 KB

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
        backup_tasks = []
        for repo in self._config.repos:
            backup_tasks.append(self.back_up_repo(repo=repo))
        await asyncio.gather(*backup_tasks)

    async def back_up_repo(self, repo: ConfigRepo):
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
        shutil.rmtree(self._DOWNLOADED_REPOS_FOLDER_PATH)

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
            overwrite=True)

        await self._yandex_disk_client.upload_by_link(archive_path, upload_link)
        # Yandex Disk slows down the download of .zip archives, so we upload as a raw file and then rename it.
        await self._yandex_disk_client.rename(file_path, f"{archive_file_name_non_zip}.zip")

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
            if not await self._yandex_disk_client.exists(full_folder_path):
                logger.info(f"Path {{{full_folder_path}}} doesn't exist. Creating...")
                await self._yandex_disk_client.mkdir(full_folder_path)
                logger.info(f"Path {{{full_folder_path}}} is successfully created.")
