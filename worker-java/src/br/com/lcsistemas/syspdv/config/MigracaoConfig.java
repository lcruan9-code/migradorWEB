package br.com.lcsistemas.syspdv.config;

/**
 * Configurações centralizadas da migração.
 * Preenchida pela MainFrame antes de executar a migração.
 */
public class MigracaoConfig {

    // ── Dados do Cliente / Empresa ────────────────────────────────────────────
    private String empresaCnpj   = "14.988.683/0001-41";  // default
    private String clienteUf     = "";
    private int    empresaId     = 1;
    private int    cidadeDefaultId  = 0;
    private int    estadoDefaultId  = 0;
    private String regimeTributario = "SIMPLES";

    // ── Banco de Origem (Firebird) ────────────────────────────────────────────
    private String fbHost        = "localhost";
    private String fbPorta       = "3050";
    private String fbArquivo     = "";
    private String fbUsuario     = "SYSDBA";
    private String fbSenha       = "masterkey";

    /**
     * Caminho para a pasta com as instalações manuais do Firebird (pasta "FIREBIRD" do projeto).
     * Se vazio, o GerenciadorFirebird tentará descobrir automaticamente.
     * Se null/vazio, o auto-start é tentado via detecção automática de pastas.
     */
    private String pastaFirebird = "";

    // ── Modo MySQL-apenas (SYSPDV — sem Firebird) ──────────────────────────────
    private boolean modoSyspdv     = false;

    /** Identificador do sistema de origem: "syspdv", "gdoor", "clipp", "host", etc. */
    private String  sistema        = "gdoor";

    // ── Banco de Destino (MySQL) ──────────────────────────────────────────────
    private String myHost        = "localhost";
    private String myPorta       = "3306";
    private String myDatabase    = "lc_sistemas";
    private String myUsuario     = "root";
    private String mySenha       = "";

    // ── Modo Web: geração de .SQL em vez de executar contra MySQL ─────────────
    private String sqlOutputPath = ""; // se preenchido, a engine gera .sql em vez de inserir

    public String getSqlOutputPath()         { return sqlOutputPath; }
    public void   setSqlOutputPath(String v) { this.sqlOutputPath = (v == null) ? "" : v; }
    public boolean isModoSqlOutput()         { return sqlOutputPath != null && !sqlOutputPath.isEmpty(); }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getEmpresaCnpj()            { return empresaCnpj; }
    public void   setEmpresaCnpj(String v)    { this.empresaCnpj = (v == null || v.isEmpty()) ? "14.988.683/0001-41" : v; }

    public String getClienteUf()              { return clienteUf; }
    public void   setClienteUf(String v)      { this.clienteUf = v; }

    public int    getEmpresaId()              { return empresaId; }
    public void   setEmpresaId(int v)         { this.empresaId = v; }

    public int    getCidadeDefaultId()        { return cidadeDefaultId; }
    public void   setCidadeDefaultId(int v)   { this.cidadeDefaultId = v; }

    public int    getEstadoDefaultId()        { return estadoDefaultId; }
    public void   setEstadoDefaultId(int v)   { this.estadoDefaultId = v; }

    public String getRegimeTributario()       { return regimeTributario; }
    public void   setRegimeTributario(String v) { this.regimeTributario = v; }

    public String getFbHost()                 { return fbHost; }
    public void   setFbHost(String v)         { this.fbHost = v; }

    public String getFbPorta()                { return fbPorta; }
    public void   setFbPorta(String v)        { this.fbPorta = v; }

    public String getFbArquivo()              { return fbArquivo; }
    public void   setFbArquivo(String v)      { this.fbArquivo = v; }

    public String getFbUsuario()              { return fbUsuario; }
    public void   setFbUsuario(String v)      { this.fbUsuario = v; }

    public String getFbSenha()                { return fbSenha; }
    public void   setFbSenha(String v)        { this.fbSenha = v; }

    public String getPastaFirebird()          { return pastaFirebird; }
    public void   setPastaFirebird(String v)  { this.pastaFirebird = (v == null) ? "" : v.trim(); }

    public String getMyHost()                 { return myHost; }
    public void   setMyHost(String v)         { this.myHost = v; }

    public String getMyPorta()                { return myPorta; }
    public void   setMyPorta(String v)        { this.myPorta = v; }

    public String getMyDatabase()             { return myDatabase; }
    public void   setMyDatabase(String v)     { this.myDatabase = v; }

    public boolean isModoSyspdv()                { return modoSyspdv; }
    public void    setModoSyspdv(boolean v)     { this.modoSyspdv = v; }

    public String  getSistema()                 { return sistema; }
    public void    setSistema(String v)         { this.sistema = v != null ? v.toLowerCase() : "gdoor"; }

    public String getMyUsuario()              { return myUsuario; }
    public void   setMyUsuario(String v)      { this.myUsuario = v; }

    public String getMySenha()                { return mySenha; }
    public void   setMySenha(String v)        { this.mySenha = v; }

    // ── Helpers para montar as JDBC URLs ──────────────────────────────────────

    /**
     * Monta a JDBC URL para o Firebird (Jaybird).
     *
     * syspdv → Firebird 2.5 na porta 3051 (ODS 11.2, sem SRP)
     * outros → Firebird 3.0 na porta fbPorta/3050 (ODS 12+, Legacy_Auth)
     */
    public String buildUrlFirebird() {
        boolean isSyspdv = "syspdv".equalsIgnoreCase(sistema);
        String porta = isSyspdv ? "3051" : fbPorta;
        // FB 2.5 não suporta SRP — Jaybird negocia legacy auth automaticamente
        // FB 3.0 precisa forçar Legacy_Auth (Ubuntu instala SYSDBA sem SRP)
        String auth = isSyspdv ? "" : "&authPlugins=Legacy_Auth";
        return "jdbc:firebirdsql://" + fbHost + ":" + porta + "/" + fbArquivo
             + "?charSet=Cp1252" + auth;
    }

    /**
     * Monta a JDBC URL para o MySQL.
     */
    public String buildUrlMySQL() {
        return "jdbc:mysql://" + myHost + ":" + myPorta + "/" + myDatabase
             + "?useSSL=false&allowPublicKeyRetrieval=true"
             + "&characterEncoding=UTF-8&serverTimezone=America%2FSao_Paulo"
             + "&allowMultiQueries=true&useOldAliasMetadataBehavior=true";
    }
}
