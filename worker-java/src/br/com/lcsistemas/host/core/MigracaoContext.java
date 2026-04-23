package br.com.lcsistemas.host.core;

import br.com.lcsistemas.host.config.MigracaoConfig;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Contexto compartilhado entre todos os steps durante a migração HOST.
 * Transporta as conexões JDBC abertas (ambas MySQL), a configuração e estatísticas.
 */
public class MigracaoContext {

    private final MigracaoConfig config;

    /** Conexão com o banco de ORIGEM (host — MySQL). */
    private Connection origemConn;

    /** Conexão com o banco de DESTINO (lc_sistemas — MySQL). */
    private Connection destinoConn;

    /** Estatísticas acumuladas por step (nome → stats). */
    private final Map<String, StepStats> stats = new LinkedHashMap<String, StepStats>();

    /** Mapa genérico para valores extras entre steps. */
    private final Map<String, Object> extras = new LinkedHashMap<String, Object>();

    public MigracaoContext(MigracaoConfig config) {
        this.config = config;
    }

    // ── Getters / Setters ──────────────────────────────────────────────────────

    public MigracaoConfig getConfig()                { return config; }

    public Connection getOrigemConn()                { return origemConn; }
    public void       setOrigemConn(Connection c)    { this.origemConn = c; }

    public Connection getDestinoConn()               { return destinoConn; }
    public void       setDestinoConn(Connection c)   { this.destinoConn = c; }

    // ── Extras genéricos ──────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public <T> T get(String key) { return (T) extras.get(key); }
    public void  put(String key, Object value) { extras.put(key, value); }

    // ── Mapas de resolução de IDs compartilhados entre steps ─────────────────

    /** codNcm (String) → id na tabela lc_sistemas.ncm */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaNcm() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaNcm");
        return m != null ? m : new java.util.HashMap<String, Integer>();
    }
    public void setMapaNcm(Map<String, Integer> m) { extras.put("mapaNcm", m); }

    /** und (String) → id na tabela lc_sistemas.unidade */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaUnidade() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaUnidade");
        return m != null ? m : new java.util.HashMap<String, Integer>();
    }
    public void setMapaUnidade(Map<String, Integer> m) { extras.put("mapaUnidade", m); }

    /** cest (String) → id na tabela lc_sistemas.cest */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaCest() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaCest");
        return m != null ? m : new java.util.HashMap<String, Integer>();
    }
    public void setMapaCest(Map<String, Integer> m) { extras.put("mapaCest", m); }

    /** grupo (String) → id na tabela lc_sistemas.categoria */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaCategoria() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaCategoria");
        return m != null ? m : new java.util.HashMap<String, Integer>();
    }
    public void setMapaCategoria(Map<String, Integer> m) { extras.put("mapaCategoria", m); }

    /** marca (String) → id na tabela lc_sistemas.fabricante */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaFabricante() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaFabricante");
        return m != null ? m : new java.util.HashMap<String, Integer>();
    }
    public void setMapaFabricante(Map<String, Integer> m) { extras.put("mapaFabricante", m); }

    /** codigo_fornecedor_host (int) → id na tabela lc_sistemas.fornecedor */
    @SuppressWarnings("unchecked")
    public Map<Integer, Integer> getMapaFornecedor() {
        Map<Integer, Integer> m = (Map<Integer, Integer>) extras.get("mapaFornecedor");
        return m != null ? m : new java.util.HashMap<Integer, Integer>();
    }
    public void setMapaFornecedor(Map<Integer, Integer> m) { extras.put("mapaFornecedor", m); }

    /** codigo_cliente_host (int) → id na tabela lc_sistemas.cliente */
    @SuppressWarnings("unchecked")
    public Map<Integer, Integer> getMapaCliente() {
        Map<Integer, Integer> m = (Map<Integer, Integer>) extras.get("mapaCliente");
        return m != null ? m : new java.util.HashMap<Integer, Integer>();
    }
    public void setMapaCliente(Map<Integer, Integer> m) { extras.put("mapaCliente", m); }

    /** id_cst → id na tabela lc_sistemas.cst */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaCst() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaCst");
        return m != null ? m : new java.util.HashMap<String, Integer>();
    }
    public void setMapaCst(Map<String, Integer> m) { extras.put("mapaCst", m); }

    /** id_cfop_host → id na tabela lc_sistemas.cfop */
    @SuppressWarnings("unchecked")
    public Map<Integer, Integer> getMapaCfop() {
        Map<Integer, Integer> m = (Map<Integer, Integer>) extras.get("mapaCfop");
        return m != null ? m : new java.util.HashMap<Integer, Integer>();
    }
    public void setMapaCfop(Map<Integer, Integer> m) { extras.put("mapaCfop", m); }

    // ── Estatísticas ──────────────────────────────────────────────────────────

    public void registrarStats(String step, int inseridos, int ignorados, int erros) {
        stats.put(step, new StepStats(step, inseridos, ignorados, erros));
    }

    public Map<String, StepStats> getStats() { return stats; }

    // ── DTO de estatísticas ───────────────────────────────────────────────────

    public static class StepStats {
        public final String nome;
        public int inseridos;
        public int ignorados;
        public int erros;

        public StepStats(String nome, int inseridos, int ignorados, int erros) {
            this.nome      = nome;
            this.inseridos = inseridos;
            this.ignorados = ignorados;
            this.erros     = erros;
        }

        public StepStats(String nome) {
            this(nome, 0, 0, 0);
        }
    }
}
