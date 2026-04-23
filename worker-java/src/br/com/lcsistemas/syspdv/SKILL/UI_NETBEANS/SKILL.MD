---
description: Manutenção de Formulários Swing e Classes Geradas pelo NetBeans GUI Builder.
---

# Skill: Atualização de UI no NetBeans (UI_NETBEANS)

Esta skill consolida as melhores práticas para desenhar, atualizar componentes e gerir o fluxo assíncrono (non-blocking UI) em aplicações criadas usando a engine de arrastar-e-soltar ("Matisse") do NetBeans IDE.

## 1. A Arquitetura em Dupla (`.java` e `.form`)

Ao criar um formulário visual (`JFrame`, `JDialog`, `JPanel`) no NetBeans, o IDE **não** armazena o layout puramente no código-fonte. Ele cria dois arquivos siameses na sua pasta `src/`:
1. `MainFrame.java`: Contém a lógica de negócio, os "Event Listeners" dos botões, e uma região restrita de layout chamada `Generated Code` (que o NetBeans protege de edições manuais).
2. `MainFrame.form`: Um arquivo XML invisível que o IDE lê para desenhar a telinha que você vê na aba "Design".

> **⚠️ Alerta de Sincronia**: Ferramentas externas (como edits de IA ou text-editors fora do NetBeans) podem facilmente injetar texto no `.java`, mas se for injetado dentro da tag resguardada `// <editor-fold defaultstate="collapsed" desc="Generated Code">`, o NetBeans poderá se corromper e apagar sua edição ao abrir, pois seu referencial cego é o `.form`.

## 2. Como Atualizar Campos e Telas do Projeto

### Opção A: Modificação Visual "Raiz" (Aba Design)
1. Dê um duplo clique no arquivo `MainFrame.java` dentro do NetBeans.
2. Clique na Aba **"Design"** (ao lado de Source, no topo).
3. Na Palette lateral, arraste os novos componentes (ex: uma nova _ComboBox_ do Regime Tributário).
4. No menu "Properties", dê um nome a ela como `cmbRegimeTributario`.
5. Salve (`Ctrl + S`). O NetBeans atualizará automaticamente o XML e refatorará o `Generated Code`.

### Opção B: Atualização Assistida por Inteligência (Código Flexível)
Ao trabalhar com programação avançada, é possível inserir "Variáveis" ou Lógica da tela independentemente do construtor bloqueado, em componentes isolados:
1. Posicone a criação manual de painéis ou de injeção de opções `cmbRegime.addItem("Opção Nova");` DENTRO do construtor da classe `MainFrame.java`, mas **após** a chamada inflexível:
   ```java
   public MainFrame() {
       initComponents(); // Intocável (GUI Builder)
       
       // Sair programando aqui:
       configurarComponentesAvulsos(); 
   }
   ```

## 3. Interfaces Longevas e Bloqueio de Tela (SwingWorker)

A GUI Java (AWT/Swing) roda numa única thread: A **EDT** (Event Dispatch Thread). 
Se, ao clicar no botão "Iniciar Migração", a nossa `MigracaoEngine` fizer a carga massiva dos milhões de registros do GDOOR conectada no banco *na mesma Thread*, a tela do usuário **vai congelar**, aparecer "(Não Respondendo)" no Windows e nenhum barra de progresso rodará!

### A Solução Nativa implementada neste Projeto
Nunca rode blocos lentos dentro do Evento do Botão (`btnIniciarActionPerformed`). Utilize o **SwingWorker**:

```java
// Em MainFrame.java ou MigracaoEngine.java
SwingWorker<Void, Void> worker = new SwingWorker<Void, Void>() {
    @Override
    protected Void doInBackground() throws Exception {
         migrar(); // O processamento infinito
         return null;
    }
};
worker.execute(); // Iniciar o loop de migração livre!
```
Essa abordagem foi injetada no núcleo de Engine deste projeto, comunicando-se com a sua Tela através da `ProgressListener` que dá "append" textual no Log Screen, livre de gargalos e sem congelar a ED Thread.
