package br.com.lcsistemas.syspdv.adaptador;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.core.MigracaoStep;
import br.com.lcsistemas.syspdv.engine.MigracaoEngine;
import br.com.lcsistemas.syspdv.step.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Estrategia de migracao SYSPDV — Firebird (origem) -> MySQL lc_sistemas (destino).
 *
 * SYSPDV e sistema Firebird. Usa ctx.getOrigemConn() para leitura do Firebird
 * e ctx.getDestinoConn() para escrita no MySQL.
 * Sem colunas temporarias no Firebird — FKs resolvidas via HashMaps em memoria.
 *
 * Ordem dos steps:
 *   1. Unidade -> 2. NCM -> 3. CstCfop (mapas) -> 4. CEST -> 5. Categoria
 *   -> 6. Fabricante -> 7. Subcategoria -> 8. Fornecedor -> 9. Produto
 *   -> 10. Cliente -> 11. Receber -> 12. Pagar
 *   -> 13. AjustePos -> 14. AjusteGeral -> 15. GrupoTributacao
 */
public class MigracaoSyspdv implements AdaptadorMigracao {

    private static final Logger LOG = Logger.getLogger(MigracaoSyspdv.class.getName());
    private static final String NOME = "MigracaoSyspdv";

    /** Steps que lêem do Firebird — usados para decidir quando liberar FB 3.0. */
    private static final Set<String> STEPS_FIREBIRD = new HashSet<>(Arrays.asList(
        "Unidade","Ncm","CstCfop","Cest","Categoria","Fabricante",
        "Subcategoria","Fornecedor","Produto","Cliente","Receber","Pagar"
    ));

    private final MigracaoContext ctx;
    private final MigracaoEngine.ProgressListener listener;
    private final AdaptadorMigracao.CancelCheck cancelCheck;

    public MigracaoSyspdv(MigracaoContext ctx,
                        MigracaoEngine.ProgressListener listener,
                        AdaptadorMigracao.CancelCheck cancelCheck) {
        this.ctx         = ctx;
        this.listener    = listener;
        this.cancelCheck = cancelCheck;
    }

    @Override
    public String getNome() { return NOME; }

    @Override
    public void executar(Connection conn) throws Exception {

        List<MigracaoStep> steps = buildSteps();
        int total = steps.size();
        log("[SYSPDV] Iniciando " + total + " steps (Firebird -> MySQL)...");

        for (int i = 0; i < total; i++) {
            if (isCancelado()) {
                throw new MigracaoException(NOME, "Cancelado no step " + (i + 1) + "/" + total);
            }

            MigracaoStep step = steps.get(i);
            String nome = step.getNome();

            stepInicio(nome, i + 1, total);
            log("[Step " + (i + 1) + "/" + total + "] " + nome + " — preparando...");

            try {
                step.prepare(ctx);
                log("[Step " + (i + 1) + "/" + total + "] " + nome + " — executando...");
                step.execute(ctx);
                ctx.getDestinoConn().commit();

                MigracaoContext.StepStats s = ctx.getStats().get(nome);
                int ins = s != null ? s.inseridos : 0;
                int ign = s != null ? s.ignorados : 0;
                int err = s != null ? s.erros     : 0;
                log("[Step " + (i + 1) + "/" + total + "] " + nome
                    + " — OK. inseridos=" + ins + " ignorados=" + ign + " erros=" + err);
                stepConcluido(nome, ins, ign, err, true);

                // Libera Firebird após o último step que o utiliza (dinâmico — respeita filtragem).
                // Steps de ajuste (AjustePos, AjusteGeral, GrupoTributacao) usam apenas H2.
                boolean esteUsaFirebird  = STEPS_FIREBIRD.contains(nome);
                boolean proximoUsaFirebird = (i + 1 < total)
                    && STEPS_FIREBIRD.contains(steps.get(i + 1).getNome());
                if (esteUsaFirebird && !proximoUsaFirebird) {
                    liberarFirebird(conn);
                }

            } catch (MigracaoException e) {
                log("[Step " + (i + 1) + "/" + total + "] " + nome + " — FALHOU: " + e.getMessage());
                try { ctx.getDestinoConn().rollback(); } catch (Exception rb) { /* ignora */ }
                step.rollback(ctx);

                MigracaoContext.StepStats s = ctx.getStats().get(nome);
                int ins = s != null ? s.inseridos : 0;
                int ign = s != null ? s.ignorados : 0;
                int err = s != null ? s.erros     : 0;
                stepConcluido(nome, ins, ign, err, false);
                step.cleanup(ctx);

                throw new MigracaoException(NOME,
                    "Migração interrompida em '" + nome + "': " + e.getMessage(), e);

            } finally {
                step.cleanup(ctx);
            }
        }

        log("[SYSPDV] Todos os steps concluídos.");
    }

    private List<MigracaoStep> buildSteps() {
        Set<String> sel  = ctx.getConfig().getTabelasSelecionadas();
        boolean     tudo = !ctx.getConfig().temSelecao(); // vazio = migrar tudo

        if (tudo) {
            log("[SYSPDV] Nenhuma seleção de tabelas — executando todos os steps.");
        } else {
            log("[SYSPDV] Tabelas selecionadas: " + sel);
        }

        List<MigracaoStep> steps = new ArrayList<>();

        // ── Steps com tabelas selecionáveis ──────────────────────────────────
        if (tudo || sel.contains("UNIDADE"))        steps.add(new UnidadeStep());
        if (tudo || sel.contains("NCM"))            steps.add(new NcmStep());
        if (tudo || sel.contains("CST"))            steps.add(new CstCfopStep());
        if (tudo || sel.contains("CEST"))           steps.add(new CestStep());
        if (tudo || sel.contains("CATEGORIA"))      steps.add(new CategoriaStep());
        if (tudo || sel.contains("FABRICANTE"))     steps.add(new FabricanteStep());
        if (tudo || sel.contains("SUBCATEGORIA"))   steps.add(new SubcategoriaStep());
        if (tudo || sel.contains("FORNECEDORES"))   steps.add(new FornecedorStep());
        if (tudo || sel.contains("PRODUTO"))        steps.add(new ProdutoStep());
        if (tudo || sel.contains("CLIENTE"))        steps.add(new ClienteStep());
        if (tudo || sel.contains("RECEBER"))        steps.add(new ReceberStep());
        if (tudo || sel.contains("PAGAR"))          steps.add(new PagarStep());

        // ── Steps de ajuste: sempre executados (integridade dos dados migrados) ─
        steps.add(new AjustePosMigracaoStep());
        steps.add(new AjusteGeralStep());

        if (tudo || sel.contains("GRUPO_TRIBUTACAO")) steps.add(new GrupoTributacaoStep());

        log("[SYSPDV] Steps a executar (" + steps.size() + "): "
            + steps.stream().map(MigracaoStep::getNome)
                   .collect(java.util.stream.Collectors.joining(", ")));

        return steps;
    }

    /**
     * Fecha a conexão Firebird e para o servidor FB 3.0 para liberar ~70MB de RAM.
     * Chamado após PagarStep (step 12) — último step que lê do Firebird.
     * Steps 13-15 (AjustePos, AjusteGeral, GrupoTributacao) usam apenas H2.
     */
    private void liberarFirebird(Connection conn) {
        // 1. Fecha a conexão JDBC (libera buffers Jaybird)
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                log("[SYSPDV] Conexão Firebird fechada após PagarStep.");
            }
        } catch (Exception e) {
            log("[SYSPDV] AVISO: erro ao fechar conn Firebird: " + e.getMessage());
        }

        // 2. Para o servidor FB 3.0 (libera ~70MB do processo)
        try {
            Process p = new ProcessBuilder("/etc/init.d/firebird3.0", "stop")
                .redirectErrorStream(true)
                .start();
            boolean done = p.waitFor(10, TimeUnit.SECONDS);
            log("[SYSPDV] FB 3.0 parado (done=" + done + ", exit=" + p.exitValue() + ") — RAM liberada.");
        } catch (Exception e) {
            log("[SYSPDV] AVISO: init.d stop falhou: " + e.getMessage() + " — tentando pkill...");
            try {
                new ProcessBuilder("pkill", "-f", "firebird3.0").start();
                log("[SYSPDV] pkill firebird3.0 enviado.");
            } catch (Exception e2) {
                log("[SYSPDV] pkill também falhou: " + e2.getMessage());
            }
        }

        // 3. Força GC para liberar heap Jaybird
        System.gc();
        log("[SYSPDV] GC forçado pós-liberação Firebird.");
    }

    private boolean isCancelado() {
        return cancelCheck != null && cancelCheck.isCancelado();
    }

    private void log(String msg) {
        LOG.info(msg);
        if (listener != null) listener.onLog(msg);
    }

    private void stepInicio(String nome, int atual, int total) {
        if (listener != null) listener.onStepInicio(nome, atual, total);
    }

    private void stepConcluido(String nome, int ins, int ign, int err, boolean ok) {
        if (listener != null) listener.onStepConcluido(nome, ins, ign, err, ok);
    }
}
