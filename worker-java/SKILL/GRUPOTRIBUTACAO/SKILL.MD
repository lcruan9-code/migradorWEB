---
description: Procedimento Funcional para a rotina Grupo de Tributação Automática.
---

# Skill: Geração do Grupo de Tributação (GRUPOTRIBUTACAO)

Esta skill documenta o último e talvez o mais importante passo lógico no final de uma migração do GDOOR para a nova base `lc_sistemas`: **O remapeamento de impostos em blocos lógicos de Grupo de Tributação atrelados ao Estado destino da loja.**

## O Problema Principal

Enquanto o GDOOR separa alíquotas de ICMS, PIS, COFINS linha por linha em atributos do produto, o sistema central organiza os produtos em `grupotributacao`. Muitas migrações deixam esse campo `NULL`, o que inviabiliza emissões de NFC-e ou NF-e.

## Como Aplicar o Ajuste

Na rotina de migração programática (ex: Java Engine `AjustePosMigracaoStep.java`), invoca-se uma Stored Procedure chamada `NOVOGRUPOTRIB2`. 

### A Stored Procedure de Tributação: `NOVOGRUPOTRIB2`

Essa procedure faz o agrupamento reverso:

1. **Agrupamento DISTINTO de Combinações Fiscais**
   A procedure vasculha TODOS os produtos do banco cuja UF condiz com a loja (passada como parâmetro, ex: `BA`) procurando combinações únicas de:
   - NCM
   - `id_cst` (Código de Situação Tributária)
   - Aliqúota ICMS
   - FCP, IPI, PIS, COFINS, Reduções.

   ```sql
   SELECT DISTINCT 
      id_ncms, id_cst, trib_icmsaliqsaida, ... 
   FROM lc_sistemas.produto 
   WHERE uf = 'BA'
   ```

2. **Criação de Entidades no `grupotributacao`**
   Se o agrupamento ainda não existe como um Grupo Oficial em `grupotributacao`, ele é inserido:
   ```sql
   INSERT INTO lc_sistemas.grupotributacao (nome, id_cst, icms_aliquota...)
   VALUES (CONCAT('NCM ', ncm, ' CST ', cst, ' ICMS ', icms, ...), ...);
   ```

3. **Re-associação Final no Produto**
   A última etapa roda um UPDATE maciço para todos os produtos casarem-se novamente (via JOIN fiscal) de volta ao respectivo id criado:
   ```sql
   UPDATE lc_sistemas.produto p
   INNER JOIN lc_sistemas.grupotributacao gt
     ON p.id_ncms = gt.id_ncm AND p.id_cst = gt.id_cst ...
   SET p.id_grupotributacao = gt.id
   WHERE p.id_grupotributacao IS NULL;
   ```

> **Atenção Prática (Correção Fundamental de Pipeline)**: A Procedure `NOVOGRUPOTRIB2` depende existencialmente de que todo o catálogo do MySQL e chaves estrangeiras (CST, CFOP, CEST) estejam consolidadas. O `script de ajuste completo.txt` **DEVE RESPIRAR PRIMEIRO!**. Se o passo da engine Java responsável por rodar o Update Integral de Cests e de Correção de Nulos (Ex: `AjustePosMigracaoStep.java`) for convocado DEPOIS do `GrupoTributacaoStep`, os NCMs/CFOPs retornarão nulos (ID 1) da base provisória, anulando as ligações do imposto e refratando os Erros do ERP nas filiais de Lojas! Rodem os Ajustes ANTES da Tributação!
---

## 🚀 Lições Aprendidas (Versão GDOOR 1.0)

Erros críticos mapeados no GDOOR 1.0 para evitar em novos sistemas:

1.  **Dependência de Procedures Externas**: Inicialmente o sistema dependia da procedure `NOVOGRUPOTRIB2` estar instalada no MySQL. Se o banco destino fosse limpo ou a procedure não existisse, a migração falhava no último passo.
    *   **Correção**: A engine agora implementa a lógica da procedure **diretamente em Java** (Step 14). Isso garante portabilidade total.
2.  **Ordem de Execução (The Fatal Gap)**: Rodar o `GrupoTributacaoStep` antes do script de ajuste completo resultava em IDs nulos (para CFOP/CST/CEST), quebrando a vinculação fiscal.
    *   **Correção**: O `GrupoTributacaoStep` deve ser o **ÚLTIMO** a rodar (Step 14/14), obrigatoriamente após o `AjustePosMigracaoStep`.
3.  **Fallback de UF**: A falta de uma UF definida causava erros de parâmetro vazio.
    *   **Correção**: Implementado fallback para `'PA'` (Pará) ou `'BA'` (Bahia) dependendo da região predominante, garantindo que o `id_estado` sempre seja resolvido.

> **Nota**: Esta documentação refere-se à **Versão 1.0** (GDOOR legado). Versões futuras do GDOOR (2024/2025) podem ter novos campos fiscais que exigirão ajustes no Step de Tributação.
