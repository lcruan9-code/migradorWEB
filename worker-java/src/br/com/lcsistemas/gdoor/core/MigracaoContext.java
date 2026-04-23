package br.com.lcsistemas.gdoor.core;

import br.com.lcsistemas.gdoor.config.MigracaoConfig;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Contexto compartilhado entre todos os steps durante uma migração.
 * Transporta as conexões JDBC abertas, a configuração e as estatísticas.
 */
public class MigracaoContext {

    private final MigracaoConfig config;

    /** Conexão com o banco de ORIGEM (Firebird). */
    private Connection origemConn;

    /** Conexão com o banco de DESTINO (MySQL). */
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

    // ── Extras genéricos (ex: columnMapper, cacheMap) ─────────────────────────

    @SuppressWarnings("unchecked")
    public <T> T get(String key) { return (T) extras.get(key); }
    public void  put(String key, Object value) { extras.put(key, value); }

    // ── Mapas de resolução de IDs (compartilhados entre steps) ───────────────

    /** codNcm (String) → id na tabela lc_sistemas.ncm */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaNcm() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaNcm");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaNcm(Map<String, Integer> m) { extras.put("mapaNcm", m); }

    /** und (String) → id na tabela lc_sistemas.unidade */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaUnidade() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaUnidade");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaUnidade(Map<String, Integer> m) { extras.put("mapaUnidade", m); }

    /** cod_cest (String) → id na tabela lc_sistemas.cest */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaCest() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaCest");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaCest(Map<String, Integer> m) { extras.put("mapaCest", m); }

    /** grupo (String upper) → id na tabela lc_sistemas.categoria */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaCategoria() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaCategoria");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaCategoria(Map<String, Integer> m) { extras.put("mapaCategoria", m); }

    /** codigo_fornecedor_gdoor (int) → id na tabela lc_sistemas.fornecedor */
    @SuppressWarnings("unchecked")
    public Map<Integer, Integer> getMapaFornecedor() {
        Map<Integer, Integer> m = (Map<Integer, Integer>) extras.get("mapaFornecedor");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaFornecedor(Map<Integer, Integer> m) { extras.put("mapaFornecedor", m); }

    /** codigo_cliente_gdoor (int) → id na tabela lc_sistemas.cliente */
    @SuppressWarnings("unchecked")
    public Map<Integer, Integer> getMapaCliente() {
        Map<Integer, Integer> m = (Map<Integer, Integer>) extras.get("mapaCliente");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaCliente(Map<Integer, Integer> m) { extras.put("mapaCliente", m); }

    /** id_cst_gdoor (st como string) → id na tabela lc_sistemas.cst */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getMapaCst() {
        Map<String, Integer> m = (Map<String, Integer>) extras.get("mapaCst");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaCst(Map<String, Integer> m) { extras.put("mapaCst", m); }

    /** id_cfop_gdoor → id na tabela lc_sistemas.cfop */
    @SuppressWarnings("unchecked")
    public Map<Integer, Integer> getMapaCfop() {
        Map<Integer, Integer> m = (Map<Integer, Integer>) extras.get("mapaCfop");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaCfop(Map<Integer, Integer> m) { extras.put("mapaCfop", m); }

    /** id_cest_gdoor (int cod_cest Firebird) → id na tabela lc_sistemas.cest */
    @SuppressWarnings("unchecked")
    public Map<Integer, Integer> getMapaCestById() {
        Map<Integer, Integer> m = (Map<Integer, Integer>) extras.get("mapaCestById");
        return m != null ? m : new java.util.HashMap<>();
    }
    public void setMapaCestById(Map<Integer, Integer> m) { extras.put("mapaCestById", m); }

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

        /** Construtor completo (usado pelo MigracaoEngine ao registrar). */
        public StepStats(String nome, int inseridos, int ignorados, int erros) {
            this.nome      = nome;
            this.inseridos = inseridos;
            this.ignorados = ignorados;
            this.erros     = erros;
        }

        /** Construtor simples — contadores começam em zero (usado pelos Steps). */
        public StepStats(String nome) {
            this(nome, 0, 0, 0);
        }
    }
}
