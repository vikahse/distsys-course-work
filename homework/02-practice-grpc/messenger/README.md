# Код решения

В этой папке надо писать весь код решения.

Единственное, что нужно соблюсти с точки зрения структуры это сохранить структуру файлов, которые уже тут лежат, а именно:
- `client.dockerfile` используется в `../docker-compose.yml`, чтобы собирать образы клиентов, вам нужно написать его самим, для запуска будет использоваться CMD/ENTRYPOINT из докерфайла
- `server.dockerfile` используется в `../docker-compose.yml`, чтобы собирать образ сервера, его так же надо написать самим
- `proto/messenger.proto` используется в тестах, поэтому его содержимое надо только расширять, перемещать файл не стоит.