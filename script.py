import asyncio

import click

from backup_client import BackupClient


@click.command()
@click.option("--yandex-disk-token", type=str, required=True)
@click.option("--github-token", type=str, required=True)
def main(yandex_disk_token: str, github_token: str):
    asyncio.run(_async_main(
        yandex_disk_token=yandex_disk_token,
        github_token=github_token
    ))


async def _async_main(yandex_disk_token: str, github_token: str):
    backup_client = BackupClient(yandex_disk_token=yandex_disk_token,
                                 github_token=github_token)
    await backup_client.back_up_repos()
    await backup_client.shutdown()


if __name__ == '__main__':
    main()
