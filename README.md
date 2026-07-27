# Migrador Web — Plataforma de Migração de ERP

SaaS que migra a base de dados de sistemas de ERP legados para o schema de
destino, de ponta a ponta: o cliente sobe o backup do banco antigo pelo portal
web e recebe de volta um dump SQL pronto para importação — sem instalar nada.

**🔗 Portal:** https://migrador-web-portal.vercel.app

## Por que existe

Migração de ERP normalmente é manual, demorada e específica por sistema. Este
projeto automatiza o pipeline (extração → transformação → carga) e suporta
múltiplos sistemas de origem (Firebird, MySQL, SQL Server, etc.) convergindo
para um schema único de destino.

## Arquitetura

```
┌─────────────────────┐   upload do backup    ┌──────────────────────┐
│  Portal (Next.js/TS) │ ────────────────────▶ │  Worker (Java, 8080) │
│  Vercel              │   POST /migrar        │  Docker              │
└─────────────────────┘                        └──────────┬───────────┘
                                                            │
                                            ┌───────────────▼───────────────┐
                                            │  MigracaoEngine                │
                                            │  • H2 in-memory (job isolado)  │
                                            │  • bootstrap do schema destino │
                                            │  • steps por sistema de origem │
                                            │  • SqlFileWriter → dump .sql   │
                                            └────────────────────────────────┘
```

- **Portal** (`portal/`) — Next.js (App Router, TypeScript). Recebe o upload,
  dispara o job e acompanha o progresso.
- **Worker** (`worker-java/`) — serviço Java containerizado. Cada job roda
  isolado em um H2 in-memory próprio, executa os *steps* de migração do sistema
  escolhido e gera o dump SQL final.
- **Infra** — `Dockerfile` + `docker-entrypoint.sh` para o worker, `render.yaml`
  para deploy do worker, Terraform (`oci-setup/`) para provisionamento em nuvem.

## Stack

Next.js · TypeScript · Java · Docker · Terraform · Firebird / MySQL / SQL Server
/ H2 · Vercel + Render / OCI

## Rodando local

```bash
# Worker (Java)
cd worker-java && ./build_and_run.ps1     # sobe o serviço em :8080

# Portal (Next.js)
cd portal && npm install && npm run dev    # :3000
```

Configuração via variáveis de ambiente — veja `worker-java/.env.example`.
Documentação de arquitetura e decisões em `SDD_MigradorWeb.md` e
`STATUS_MIGRACAO.md`.

## Licença

[MIT](LICENSE) © Ruan Paiva
