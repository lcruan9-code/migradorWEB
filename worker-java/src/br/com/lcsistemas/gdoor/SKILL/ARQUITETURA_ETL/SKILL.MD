---
description: Guia de Modelagem de Sequenciamento ETL para Migrações de Banco de Dados.
---

# Skill: Arquitetura e Orquestração ETL (ARQUITETURA_ETL)

Esta skill documenta os princípios fundamentais e a ordem de execução a ser seguida em qualquer projeto de Migração de Dados (Migration Engine) que possua dependências relativas (Foreign Keys e Regras Fiscais) entre as entidades carregadas.

## O Desafio da Race-Condition (Condição de Corrida)

Em engines em Java que puxam dados de um sistema para o outro, criam-se classes separadas para cada tabela (`ProdutoStep`, `ClienteStep`). No entanto, algumas tabelas de ajustes ou procedures consolidadas baseiam-se em catálogos complementares (como Unidades e CEST). 

Se um Passo (Step) que invoca uma Stored Procedure de consolidação (como o recálculo fiscal) rodar antes do Passo que insere os complementos no banco, todos os produtos ficarão com conexões nulas.

## Padrão Ouro de Orquestração de Steps

Para os seus futuros projetos de Java Swing ou Spring Boot que englobem Migração Estruturada, utilize a seguinte ordem canônica na classe Orchestrator (`MigracaoEngine.java`):

1. **Step Dicionários Base**: `NcmStep`, `CestStep`, `UnidadeStep`, `CstCfopStep`. Eles fornecem as opções que preencherão as comboboxes e Foreign Keys de tudo que viermos a seguir.
2. **Step de Entidades Menores**: `CategoriaStep`, `FabricanteStep`.
3. **Step de Parceiros**: `ClienteStep`, `FornecedorStep`.
4. **Step Core (Núcleo)**: `ProdutoStep`. Aqui, o produto puxará os IDs carregados nos Passos 1 e 2. Se algum ID faltar no "De/Para", instancie um código "Default", como `id=1` ou `0`, usando `getOrDefault(...)` num mapa de memória para contornar NULL exceptions.
5. **Step de Lançamentos**: `PagarStep`, `ReceberStep`, `EstoqueStep` (depende visceralmente dos IDs criados nos Steps 3 e 4).
6. **Step de Ajustes Nativos (O DUMP)**: `AjustePosMigracaoStep`. Roda scripts SQL gigantes (`.txt` ou `.sql`) via JDBC puro, inserindo as amarrações do ERP GDOOR, consertando campos defasados e aplicando `UPDATE` de regras de negócio estáticas (ex: bairro para Centro, null para 0.00).
7. **Step Final Tributário/Compilação**: `GrupoTributacaoStep` (A Etapa Magna). Aciona `Functions` ou `Stored Procedures` (`CALL procedure(uf)`) para rodar em cima da base **completamente** populada e higienizada. É ela que amarrará a classificação fiscal da loja baseada nos Cests limpos trazidos do Passo 6!

> **Regra Matriz**: Jamais invoque um recálculo analítico no banco antes de rodar os scripts literais de preenchimento (`.txt` de ajuste ou `DUMPS` originais do DBeaver). A compilação é sempre a última linha do processo.
