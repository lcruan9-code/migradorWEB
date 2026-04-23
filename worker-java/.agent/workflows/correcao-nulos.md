---
name: correcao-nulos
description: Protocolo de injeção de Fallbacks Mappers (Orphan Defaults) na gravação de Tabelas. Use quando o Agente precisar transferir dados legados via Map/Dict para destinos Relacionais Rígidos.
argument-hint: [classe do DAO ou Mapeador de Inserção]
allowed-tools: Read, Edit
context: fork
agent: general-purpose
---

O usuário está rodando transformações numéricas no arquivo: **$ARGUMENTS**

---

## Protocolo obrigatório

### PASSO 1 — Localizar inserções de Keys via Metadados
Identifique onde a interface do DAO recolhe um ForeignKey de um Mapa para preencher um Record usando a ferramenta Read.

### PASSO 2 — Aplicar Blindagem de Constraints
Quando o Array Original for desprovido de chaves equivalentes, a chamada simples de `map.get('key')` renderá NULL no Prepared Statement, quebrando a integridade do Destino.

O Agente DEVE propor em todas inserções Mapeadas o Helper Abaixo:
```java
// Implemente um Helper Seguro no Scopo do DAO:
private String getSafeId(String mapValue) {
    return (mapValue != null && !mapValue.trim().isEmpty()) ? mapValue : "1";
}
// Chame nos Sets:
st.setString(1, getSafeId(mapaCest.get(codigo)));
```

### PASSO 3 — Validação de Exceções Tributárias
Para CST e CFOPs provindos do GDOOR:
- NCM Padrão se Falho é `00000000` (ID de fallback usual: 1).
- Unidade Padrão se Nula é UID=1.
Siga esses roteiros sempre na formatação dos inserts.
