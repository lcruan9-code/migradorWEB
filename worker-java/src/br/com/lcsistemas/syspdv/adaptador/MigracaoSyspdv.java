package br.com.lcsistemas.syspdv.adaptador;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.core.MigracaoStep;
import br.com.lcsistemas.syspdv.engine.MigracaoEngine;
import br.com.lcsistemas.syspdv.step.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
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

                // Após PagarStep (i=11, step 12), o Firebird não é mais necessário.
                // Steps 13-15 (AjustePos, AjusteGeral, GrupoTributacao) usam apenas H2.
                // Fechar a conexão + parar FB 3.0 libera ~70MB antes do AjusteGeral.
                if (i == 11) {
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
        return Arrays.<MigracaoStep>asList(
            new UnidadeStep(),
            new NcmStep(),
            new CstCfopStep(),
            new CestStep(),
            new CategoriaStep(),
            new FabricanteStep(),
            new SubcategoriaStep(),
            new FornecedorStep(),
            new ProdutoStep(),
            new ClienteStep(),
            new ReceberStep(),
            new PagarStep(),
            new AjustePosMigracaoStep(),
            new AjusteGeralStep(),
            new GrupoTributacaoStep()
        );
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
