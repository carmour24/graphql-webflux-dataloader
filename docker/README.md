## Postgres docker file for testing

Build image (a template for a container, which is a running instance) with test database installed (see `/docker/postgres/create-db.sql`)

```
> docker build -t test/gql-wf-dl postgres
```

Create and run a container from the image above

```
> docker run --name test-gql-wf-dl -p 5432:5432 test/gql-wf-dl
```