# Deploy Web Guide

This copy is the web-ready branch of the project.

## Goal

Keep the current syspdv base safe and deploy the system in two parts:

- Portal on Vercel
- Worker Java on Zeabur

## Why this split

- Vercel is a good fit for the Next.js portal.
- Zeabur accepts Dockerfile deployments and can run the Java worker as a container.
- The current worker still depends on local temp files and Java runtime behavior, so it should not be forced into a serverless-only model.

## Portal on Vercel

Use the `portal/` folder as the Vercel project root.

Required environment variables:

- `NEXT_PUBLIC_WORKER_URL`
- `WORKER_URL`

Recommended value:

- set both to the public URL of the Zeabur worker

What to check after deploy:

- the state list loads
- the city list loads
- file upload works
- the download route proxies to the worker

## Worker on Zeabur

Use the `worker-java/` folder as the service root.

The Dockerfile is already prepared in:

- `worker-java/Dockerfile`

The container exposes:

- `PORT`

Zeabur should set `PORT` automatically, but the app also falls back to `8080`.

What to check after deploy:

- `GET /health`
- `GET /api/estados`
- `GET /api/cidades?uf=SP`
- `POST /api/processar`
- `GET /api/status/{jobId}`
- `GET /api/download/{jobId}`

## Recommended first test

1. Deploy the worker first on Zeabur.
2. Copy the public worker URL.
3. Set that URL in Vercel as `NEXT_PUBLIC_WORKER_URL` and `WORKER_URL`.
4. Deploy the portal on Vercel.
5. Open the portal and test UF, city, upload, and download.

## Current design note

The current functional state uses H2 only as an internal staging step for SQL generation. The final deliverable remains the MySQL 5.5.38-style dump based on `banco_novo.sql`.

## Safety note

Keep the local base copy untouched:

- `C:\Users\ruanp\NETBENAS - COWORK\migrador-web-base-syspdv`

Use this web-ready copy only for cloud adjustments and deployment experiments.
