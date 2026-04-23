---
name: swing-ux
description: Guia mudanças de UX em telas Swing do projeto (NetBeans GUI Builder). Use quando precisar alterar layout, componentes, eventos ou propriedades visuais de telas .form/.java.
argument-hint: [nome da tela ou componente]
allowed-tools: Read, Grep, Glob, Edit
context: fork
agent: general-purpose
---

O usuário quer fazer uma mudança de UX na tela: **$ARGUMENTS**

---

## Protocolo obrigatório

### PASSO 1 — Localizar os arquivos da tela

Use Glob para encontrar os arquivos correspondentes:
- `src/**/$ARGUMENTS.form`
- `src/**/$ARGUMENTS.java`

Se $ARGUMENTS for vago (ex: "tela de cliente"), use Grep para localizar pelo nome parcial.

---

### PASSO 2 — Ler ambos os arquivos ANTES de qualquer análise

Leia o `.form` completo (XML do GUI Builder) e o `.java` completo.

Identifique e registre:
- Lista de componentes declarados no `.form` (atributo `name` de cada `<Component>`)
- Bloco de declarações auto-gerado no final do `.java` (tudo após `// Variables declaration`)
- Eventos já vinculados (`<EventHandler>` no `.form`)

---

### PASSO 3 — Classificar o tipo de mudança solicitada

Antes de propor qualquer alteração, determine a categoria:

#### A. Propriedade visual
Exemplos: tamanho, cor, fonte, texto de label, tooltip, alinhamento, visibilidade padrão.
- Arquivo afetado: **somente `.form`**
- Nunca editar `initComponents()` no `.java` manualmente.

#### B. Troca de tipo de componente
Exemplos: `JTextField` → `JFormattedTextField`, `JButton` → `JToggleButton`.
- Arquivos afetados: **`.form` + declaração no `.java`**
- No `.form`: alterar a tag XML do componente (atributo `class`)
- No `.java`: alterar o tipo na declaração do bloco `// Variables declaration`
- **Jamais** propor edição dentro de `initComponents()` — o NetBeans regenera esse bloco ao abrir o form editor.
- Após a troca: verificar se há chamadas ao componente no `.java` que dependem de métodos do tipo antigo.

#### C. Comportamento / evento
Exemplos: adicionar `ActionListener`, `FocusListener`, `KeyListener`, lógica de validação inline.
- O **binding do evento** fica no `.form` (tag `<EventHandler>`)
- O **corpo do método** fica no `.java`, fora do bloco `initComponents()`
- Nunca duplicar listeners: verificar se já existe handler para o mesmo evento antes de adicionar.

#### D. Novo componente
- Descrever ao usuário o componente a ser adicionado via NetBeans GUI Builder (não gerar código de declaração manualmente).
- Somente após confirmação do usuário de que adicionou no GUI Builder, propor código do evento/comportamento no `.java`.
- Exceção: se o usuário pedir para adicionar o XML no `.form` diretamente, seguir o padrão exato dos componentes existentes no arquivo.

#### E. Remoção de componente
- Verificar todas as referências ao componente no `.java` (fora do bloco auto-gerado) antes de remover.
- Listar cada referência encontrada e confirmar com o usuário antes de proceder.

---

### PASSO 4 — Apresentar o plano antes de editar

Antes de qualquer `Edit`, exibir:

```
PLANO DE MUDANÇA
================
Tela:        <nome da tela>
Categoria:   <A/B/C/D/E>
Arquivos:    <lista dos arquivos que serão alterados>

Mudanças previstas:
  1. [arquivo:linha] <descrição da alteração>
  2. [arquivo:linha] <descrição da alteração>

Impactos identificados:
  - <método/componente que pode ser afetado>
  - <nenhum, se não houver>
```

Aguardar confirmação do usuário antes de executar os Edits.

---

### PASSO 5 — Executar e reportar

Após as edições, exibir um relatório final:

```
RELATÓRIO DE MUDANÇA
====================
✔ [arquivo:linha] <o que foi alterado>
✔ [arquivo:linha] <o que foi alterado>

Próximos passos manuais (se houver):
  - <ação que o usuário precisa fazer no NetBeans GUI Builder>
  - <ex: "Abrir o .form no editor para regenerar initComponents()">
```

---

## Restrições absolutas

- **Nunca** editar dentro do bloco `// GEN-BEGIN:initComponents` ... `// GEN-END:initComponents`
- **Nunca** editar o bloco `// Variables declaration` ... `// End of variables declaration` sem antes classificar como categoria B ou E e confirmar com o usuário
- **Nunca** sugerir componente Swing externo (third-party) sem validar JAR disponível no projeto
- Manter compatibilidade com **Java 8**
- Manter compatibilidade com **NetBeans GUI Builder** — não propor padrões que o form editor não reconheça
