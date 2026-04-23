# Passo a Passo de Deploy no Render

Base segura:

- `C:\Users\ruanp\NETBENAS - COWORK\migrador-web-base-syspdv`

Cópia preparada para web:

- `C:\Users\ruanp\NETBENAS - COWORK\migrador-web-web-ready`

## 1) Preparar o Git local

Entre na pasta da cópia web e rode:

```powershell
cd "C:\Users\ruanp\NETBENAS - COWORK\migrador-web-web-ready"
git init
git add .
git commit -m "Base web ready para Render"
```

Se você ainda não criou o repositório no GitHub:

1. Abra o GitHub.
2. Crie um repositório novo vazio.
3. Copie a URL do repositório.

Depois conecte e envie:

```powershell
git branch -M main
git remote add origin https://github.com/SEU-USUARIO/SEU-REPO.git
git push -u origin main
```

## 2) Render: criar o projeto pelo GitHub

1. Entre no painel do Render.
2. Clique em `New`.
3. Selecione `Blueprint`.
4. Conecte sua conta GitHub.
5. Escolha o repositório que contém o `render.yaml`.
6. O Render vai ler o arquivo `render.yaml` na raiz do repositório.
7. Confirme a criação dos serviços.

## 3) O que o Render vai criar

O blueprint já define dois serviços:

- `migrador-worker`
- `migrador-portal`

O portal conversa com o worker pela URL pública do próprio worker, mas isso fica escondido atrás das rotas internas do Next.js.

## 4) Serviços no Render

### `migrador-worker`

Esse serviço usa:

- `worker-java/Dockerfile`

O que conferir no painel:

- tipo do serviço: `Web Service`
- runtime: `Docker`
- health check: `/health`

Variáveis que o blueprint já define:

- `PORT=8080`

### `migrador-portal`

Esse serviço usa:

- `portal/`

O que conferir no painel:

- tipo do serviço: `Web Service`
- runtime: `Node`
- build command: `npm ci && npm run build`
- start command: `npm start`
- health check: `/`

Variável que o blueprint já define:

- `WORKER_URL` apontando para a URL pública do worker no Render

## 5) Ordem certa de validação

Depois do deploy, teste nesta ordem:

1. Abra a URL do worker e teste:
   - `/health`
   - `/api/estados`
   - `/api/cidades?uf=SP`
2. Abra a URL do portal.
3. Teste:
   - seleção de estado
   - carregamento de cidade
   - upload do arquivo
   - processamento
   - download do `.sql`

## 6) Se algo falhar

- Se o worker não subir, veja os logs do serviço `migrador-worker`.
- Se o portal abrir mas não carregar cidades, veja se o blueprint criou o `WORKER_URL` corretamente.
- Se o download falhar, veja os logs da rota `/api/download/[jobId]` no portal.

## 7) Comandos úteis para revisão local

### Portal

```powershell
cd "C:\Users\ruanp\NETBENAS - COWORK\migrador-web-web-ready\portal"
npm install
npm run build
```

### Worker

```powershell
cd "C:\Users\ruanp\NETBENAS - COWORK\migrador-web-web-ready\worker-java"
```

O worker vai rodar via Docker no Render, então no local o foco é só validar código e estrutura.

## 8) Nota de segurança

Não mexa na base congelada:

- `C:\Users\ruanp\NETBENAS - COWORK\migrador-web-base-syspdv`

Faça qualquer experimento só na cópia web:

- `C:\Users\ruanp\NETBENAS - COWORK\migrador-web-web-ready`
