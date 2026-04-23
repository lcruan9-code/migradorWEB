---
description: Higienização Definitiva de Telefone — Java Inline via StepBase (sem dependência de procedure MySQL)
---

# Skill: Higienização de Telefone (TELEFONE)

Esta skill documenta a solução **definitiva e validada** para limpeza de campos de telefone oriundos de bancos sujos (GDOOR/Firebird, Datages, etc.) que não aplicavam máscara na entrada de dados.

---

## O Problema Real

Campos `TELEFONE` e `CELULAR` do Firebird chegam em formatos completamente livres:

```
(0xx) 9-1234-5678
(XX)9-98765-4321
XX 99999-1111
0xx75 9 8765-4321
775 9999-1234
(75) 9.8765-1234
9999-1234
```

### Por que a abordagem antiga falhou

O método `limparTelefone` original fazia substituição de prefixos hardcoded:

```java
// ❌ ABORDAGEM ANTIGA — NÃO USE
String[] prefixos = {"(0xx", "(XX)9-", "XX", "775", "XX ", "xx ", "0xx ", ...};
for (String p : prefixos) tel = tel.replace(p, "");
```

**Falha crítica:** `tel.replace("XX", "")` substitui `"XX"` em **qualquer posição** da string, não só no prefixo. Um número como `"(75) 9-XXXX-1234"` vira `"(75) 9--1234"` — ainda com caracteres não-numéricos, ainda podendo exceder o limite da coluna MySQL.

**Erro resultante:**
```
Data truncation: Data too long for column 'tel_comercial' at row 1
```

---

## A Solução Definitiva — Java Inline no StepBase

> **Regra de Ouro**: Nunca dependa de procedure MySQL para higienização. Se a procedure for deletada do banco destino, a migração quebra. Implemente inline em `StepBase`.

### 1. Métodos adicionados ao `StepBase.java`

```java
/**
 * Extrai apenas os dígitos de uma string.
 * Espelha a FUNCTION MySQL extrair_numeros() — mesma lógica, sem dependência de banco.
 * Ex: "(0xx) 9-1234-5678" → "09912345678"
 */
protected static String extrairNumeros(String texto) {
    if (texto == null) return "";
    return texto.replaceAll("[^0-9]", "");
}

/**
 * Limpa um telefone extraindo somente dígitos e truncando ao limite da coluna.
 * @param tel   Valor bruto vindo do Firebird (qualquer formato)
 * @param maxLen Tamanho máximo da coluna destino no MySQL (ex: 20)
 */
protected static String limparTelefone(String tel, int maxLen) {
    if (tel == null || tel.isEmpty()) return "";
    String digits = extrairNumeros(tel);
    if (digits.isEmpty()) return "";
    return digits.length() > maxLen ? digits.substring(0, maxLen) : digits;
}
```

### 2. Como usar em qualquer Step

```java
// Nos Steps que processam telefone, substitua qualquer limpeza manual por:
String tel = limparTelefone(nvl(rsFb.getString("TELEFONE")), 20);
String cel = limparTelefone(nvl(rsFb.getString("CELULAR")),  20);
String fax = limparTelefone(nvl(rsFb.getString("FAX")),      20);
```

O `maxLen` deve refletir o `VARCHAR(N)` da coluna no banco destino:

| Coluna MySQL       | maxLen recomendado |
|--------------------|--------------------|
| `telefone`         | 20                 |
| `tel_comercial`    | 20                 |
| `fone`             | 20                 |
| `celular`          | 20                 |

### 3. Equivalente MySQL (para referência / pós-migração)

Caso precise da limpeza também no banco destino (ex: ajuste de dados já migrados), use a function nativa:

```sql
DELIMITER $$
DROP FUNCTION IF EXISTS `extrair_numeros` $$
CREATE DEFINER=`root`@`localhost` FUNCTION `extrair_numeros`(documento VARCHAR(255))
RETURNS VARCHAR(255) CHARSET latin1 DETERMINISTIC
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE c CHAR(1);
    DECLARE resultado VARCHAR(255) DEFAULT '';
    DECLARE len INT DEFAULT LENGTH(documento);
    WHILE i <= len DO
        SET c = SUBSTR(documento, i, 1);
        IF c BETWEEN '0' AND '9' THEN
            SET resultado = CONCAT(resultado, c);
        END IF;
        SET i = i + 1;
    END WHILE;
    RETURN resultado;
END $$
DELIMITER ;
```

**Uso pós-migração (opcional):**
```sql
UPDATE lc_sistemas.cliente
SET telefone     = extrair_numeros(telefone),
    tel_comercial = extrair_numeros(tel_comercial)
WHERE id > 2;
```

---

## Checklist de Duplicação para Novo Sistema

Ao iniciar uma migração para outro sistema de origem, valide este checklist:

- [ ] `StepBase` contém `extrairNumeros(String)` e `limparTelefone(String, int)`
- [ ] Todos os Steps que leem campos de telefone usam `limparTelefone(raw, maxLen)` — não replace manual
- [ ] O `maxLen` foi conferido no DDL do banco destino (`VARCHAR(?)` da coluna)
- [ ] **Não há** método privado `limparTelefone()` duplicado nos Steps (herdar do `StepBase`)
- [ ] Campos `FAX` também passam por `limparTelefone` se mapeados para coluna com limite

---

## Lição Aprendida

> Strings de telefone de sistemas legados devem ser tratadas como **texto livre de tamanho desconhecido**. A única higienização segura é extrair somente dígitos (`[^0-9]` → `""`). Qualquer outra abordagem baseada em padrões parciais vai falhar para algum registro real.
