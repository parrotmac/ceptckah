# ceptckah / Cert Scanner

This is a hacked-up version of [https://github.com/govau/certwatch](certwatch).

The goal of this is to learn about Certificate Transparency logs.

### Quickstart
```bash
 docker run -d --rm --name ceptckah -p 5435:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:14-alpine
 go run .
```