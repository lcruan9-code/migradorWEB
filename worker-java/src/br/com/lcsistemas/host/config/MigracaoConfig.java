package br.com.lcsistemas.host.config;

/**
 * Configurações centralizadas da migração HOST.
 * Origem: banco Firebird (.fdb) — lido via Jaybird JDBC.
 * Destino: banco MySQL (lc_sistemas).
 * Preenchida pela MainFrame antes de executar a migração.
 */
public class MigracaoConfig {

    // ── Dados do Cliente / Empresa ────────────────────────────────────────────
    private String empresaCnpj      = "";
    private String clienteUf        = "";
    private int    empresaId        = 1;
    private int    cidadeDefaultId  = 1532; // CARUARU/PE (padrão script)
    private int    estadoDefaultId  = 16;    // PE
    private String regimeTributario = "SIMPLES";

    // ── Banco de Origem (Firebird) ────────────────────────────────────────────
    private String origemHost       = "localhost";
    private String origemPorta      = "3050";
    private String origemArquivoFdb = "";
    private String origemUsuario    = "SYSDBA";
    private String origemSenha      = "masterkey";

    // ── Banco de Destino (lc_sistemas — MySQL) ────────────────────────────────
    private String myHost        = "localhost";
    private String myPorta       = "3306";
    private String myDatabase    = "lc_sistemas";
    private String myUsuario     = "root";
    private String mySenha       = "";

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getEmpresaCnpj()              { return empresaCnpj; }
    public void   setEmpresaCnpj(String v)      { this.empresaCnpj = v == null ? "" : v; }

    public String getClienteUf()               { return clienteUf; }
    public void   setClienteUf(String v)       { this.clienteUf = v == null ? "" : v; }

    public int    getEmpresaId()                { return empresaId; }
    public void   setEmpresaId(int v)           { this.empresaId = v; }

    public int    getCidadeDefaultId()          { return cidadeDefaultId; }
    public void   setCidadeDefaultId(int v)     { this.cidadeDefaultId = v; }

    public int    getEstadoDefaultId()          { return estadoDefaultId; }
    public void   setEstadoDefaultId(int v)     { this.estadoDefaultId = v; }

    public String getRegimeTributario()         { return regimeTributario; }
    public void   setRegimeTributario(String v) { this.regimeTributario = v; }

    // Origem (Firebird)
    public String getOrigemHost()                 { return origemHost; }
    public void   setOrigemHost(String v)         { this.origemHost = v; }

    public String getOrigemPorta()                { return origemPorta; }
    public void   setOrigemPorta(String v)        { this.origemPorta = v; }

    public String getOrigemArquivoFdb()           { return origemArquivoFdb; }
    public void   setOrigemArquivoFdb(String v)   { this.origemArquivoFdb = v == null ? "" : v; }

    public String getOrigemUsuario()              { return origemUsuario; }
    public void   setOrigemUsuario(String v)      { this.origemUsuario = v; }

    public String getOrigemSenha()                { return origemSenha; }
    public void   setOrigemSenha(String v)        { this.origemSenha = v; }

    // Destino
    public String getMyHost()                   { return myHost; }
    public void   setMyHost(String v)           { this.myHost = v; }

    public String getMyPorta()                  { return myPorta; }
    public void   setMyPorta(String v)          { this.myPorta = v; }

    public String getMyDatabase()               { return myDatabase; }
    public void   setMyDatabase(String v)       { this.myDatabase = v; }

    public String getMyUsuario()                { return myUsuario; }
    public void   setMyUsuario(String v)        { this.myUsuario = v; }

    public String getMySenha()                  { return mySenha; }
    public void   setMySenha(String v)          { this.mySenha = v; }

    // ── Helpers para montar as JDBC URLs ──────────────────────────────────────

    /**
     * Monta a JDBC URL para o banco de ORIGEM (Firebird via Jaybird).
     * Formato: jdbc:firebird://host:porta/caminho/para/arquivo.fdb
     *          ?encoding=ISO8859_1
     */
    public String buildUrlOrigem() {
        // Jaybird aceita barra invertida no caminho no Windows; usar replace para garantir
        String fdb = origemArquivoFdb.replace("\\", "/");
        return "jdbc:firebirdsql://" + origemHost + ":" + origemPorta + "/" + fdb
             + "?encoding=ISO8859_1&charSet=ISO-8859-1";
    }

    /** Monta a JDBC URL para o banco de DESTINO (lc_sistemas — MySQL). */
    public String buildUrlMySQL() {
        return "jdbc:mysql://" + myHost + ":" + myPorta + "/" + myDatabase
             + "?useSSL=false&allowPublicKeyRetrieval=true"
             + "&characterEncoding=UTF-8&serverTimezone=America%2FSao_Paulo"
             + "&allowMultiQueries=true&useOldAliasMetadataBehavior=true";
    }
}
