---
description: Práticas definitivas para Leitura de Dumps SQL e Parser massivo no Java.
---

# Skill: Parser Nativo de Scripts JDBC e Charsets (PARSER_JDBC)

Esta skill documenta o formato definitivo para ler centenas de milhares de linhas de código SQL e DUMPS exportados via DBeaver, HeidiSQL ou MySQL Workbench, e rodá-los na sua engine sem quebras de memória e sem percursos de laços corrompidos.

## O Problema dos Lotes Colossais e o UTF-8
Bancos de dados legados do Windows (como `.fdb` do Firebird do GDOOR) muitas vezes exportam strings que utilizam codificação proprietária da Microsoft (`Windows-1252`) ou o padrão `ISO-8859-1`. 

Usar o leitor padrão do Java moderno (`UTF-8`) resultará em perdas invisíveis (Silent Character Dropping): no momento que caracteres como *"ção"* ou *"Graus °"* são atingidos, a rotina esnoba a linha, anulando lotes inteiros de produtos da sua migração!

### Solução 1: Especificação Estrita de Codificação
Ao varrer arquivos vindos de FileSystems Windows na migração, sempre acople a conversão do Charset ativamente:
```java
// Leitura garantida que tolerará caracteres especiais legados do Windows ANSI (ISO-8859-1)
byte[] encoded = java.nio.file.Files.readAllBytes(arquivo.toPath());
String sqlTotal = new String(encoded, java.nio.charset.StandardCharsets.ISO_8859_1);
```

## O Problema do Delimitador
Quando scripts possuem milhares de "INSERTS" de tabela que o DBA concatenou (um único `UPDATE` ou `INSERT VALUES (1)..(99000);`), quebrar esse script em laços como `for (linha)` ou `while (linha.endsWith(";"))` sobrecarrega a String Builder da JVM, podendo fragmentar partes do array em que "fecha o parenteses". E JDBC por padrão **proíbe** envio de múltiplos statements com `;` na mesma query de texto.

### Solução 2: A Chave Mestra Oculta `allowMultiQueries`
A solução elegante que elimina 100% o travamento em grandes pipelines e imita o terminal real não é escrever um Parser java gigantesco; mas ligar a diretiva nativa da Connection Factory do MySQL da sua engine: `&allowMultiQueries=true`.

Inclua na montagem de Conexão do seu projeto Java (na string originária de URL de driver JDBC):
```java
public String buildUrlMySQL() {
    return "jdbc:mysql://" + host + ":" + porta + "/" + database
         + "?useSSL=false&allowPublicKeyRetrieval=true"
         + "&characterEncoding=UTF-8&serverTimezone=America%2FSao_Paulo"
         + "&allowMultiQueries=true"; // <---- ESSA MÁGICA
}
```

```java
// O Código Matador e Elegante Pós-Config:
try (Statement st = conexaoDestino.createStatement()) {
    // Agora o Java despeja 300MB de texto SQL de uma tacada, e o Engine Nativo em C++ do MySQL compila sem dó!
    st.execute(stringInteiraDoArquivoTxt);
}
```

> **Atenção sobre o MultiQuery**: Apesar dessa flag resolver `DUMPS`, a API JDBC nativa não compreende instruções proprietárias de terminal e client-side como `DELIMITER $$`. O `allowMultiQueries=true` foi feito para sintaxe ANSI SQL universal. Se houver DELIMITERS, é necessário cortá-los e fazer scripts procedimentais simples ou isolar o código de Functions/Procedures num leitor auxiliar simples fora do MultiQuery.
