---
description: Conversão de Lógicas CST ICMS entre Regimes Tributários e uso inteligente de CFOPs.
---

# Skill: Mapeamento Tributário Avançado (MAPEAMENTO_TRIBUTARIO)

Esta skill documenta as heurísticas e transformações usadas para processamento automático de Tributos Fiscais ao migrar sistemas legados para padrões de Tabela de Carga modernos de NF-e e NFC-e, em especial convertendo tabelas separadas.

## Cenário
Frequentemente, sistemas legados ou desatualizados salvam alíquotas misturadas com Regimes no mesmo campo, ou simplesmente utilizam regras cegas. No nosso Migration Engine, adotamos um flag central na UI: **Regime Tributário** (`SIMPLES` ou `NORMAL`).

Dependendo deste Regime, os números armazenados pelo sistema de origem (Ex: `ST: 400`) não condizem com a SEFAZ destino e a emissora de vendas no Ponto de Venda. A correção programática (Feita tradicionalmente no `ProdutoStep`) executa a modelagem a seguir:

## Regras de Roteamento Simples vs Normal

Ao escanear a `Tabela de Carga` ou CST Originais, crie métodos estáticos na sua engine com conversão por Switch/Case pautada no Tipo de Loja.

### Regime NORMAL (Lucro Real ou Presumido)
Utiliza a tabela oficial (A). Mantemos uma lógica de desvalorização e limpeza:
* `CST 400` → Reduz-se para `41` (Isenta/Não tributada).
* `CST 500` → Reduz-se para `60` (ICMS cobrado anteriormente por substituição tributária).
* `CST 300`, `102` geram NullPointerException ou rejeição em malha Normal, convertem-se em Isentos / `41`.

### Regime SIMPLES O NACIONAL
Utiliza os Códigos de Situação da Operação no Simples Nacional (CSOSN - Tabela B).
É essencial processar e transmutar todos os CSTs oriundos de fontes sujas no novo formato 3-dígitos:
* Transforme `0`, `10`, `20`, `40` e `41` em CSOSN **`102`** (Tributada pelo Simples s/ permissão de crédito).
* Transforme `50` e `51` em CSOSN **`300`** (Imune).
* Transforme valores altos ou difusos como `60`, `70`, `500` diretamente em CSOSN **`500`** (ICMS cobrado antes por ST).
* Transforme alíquotas estranhas e não mapeadas para **`900`** (Outras Operações).

## Roteamento Analítico de CFOPs

Os CFOPs orientam o tipo da venda e do estoque. Num ambiente padronizado da nossa Engine, limitamos o escopo aos números genéricos Estaduais do Comércio para sanar os produtos na Tabela `lc_sistemas`:

* **CFOP Padrão Genérico**: `289`. Esse será associado à maioria dos produtos recém migrados cujo CST represente mercadoria livre.
* **CFOP Substituição (ST)**: `355`. Apenas utilize esse ID de CFOP no Java se o CST final do seu produto bateu nos valores que clamam por Substituição (`CST 60` no Normal ou `CST 500` no Simples). Essa trava de código em 355 isenta o vendedor de multas absurdas do fisco!

> **Dica Prática para Novo Código Java**:  
> No seu `ProdutoStep`, crie um boolean local: `boolean isSimples = "SIMPLES".equals(config.getRegimeTributario());` e ramifique as regras. A amarração `getOrDefault(codCST, 1)` fará o default pra 1 (código nulo) permitindo que as procedures SQL tributárias corrijam na nuvem final!
