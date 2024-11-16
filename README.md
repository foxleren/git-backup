# git-backup

Простой скрипт для регулярного бэкапа репозиториев на Яндекс.Диск на случай бана аккаунта.

## Памятка для меня же через N времени



### Необходимые ENV

```
GIT_BACKUP_GITHUB_TOKEN
GIT_BACKUP_YANDEX_DISK_TOKEN
```

### Github

Для работы с апи Гитхаба нужно создать токен [тут](https://github.com/settings/tokens/new)
с правами "repo (Full control of private repositories)". Токен необходимо периодически обновлять.


### Яндекс.Диск

Для работы с апи Яндекс.Диск необходимо создать приложение [тут](https://oauth.yandex.ru/) и выдать ему права
"cloud_api:disk.app_folder (Доступ к папке приложения на Диске)". Токен так же периодически обновлять. 

Токен можно получить по следующей ссылке:
```
https://oauth.yandex.ru/authorize?response_type=token&client_id=<CLIENT_ID>
```

[Документация API Яндекс.Диск](https://yandex.ru/dev/disk-api/doc/ru/)
