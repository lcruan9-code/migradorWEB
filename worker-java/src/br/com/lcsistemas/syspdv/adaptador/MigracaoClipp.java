package br.com.lcsistemas.syspdv.adaptador;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.core.MigracaoStep;
import br.com.lcsistemas.syspdv.engine.MigracaoEngine;
import br.com.lcsistemas.syspdv.step.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Estratégia de migração Clipp (Firebird) → lc_sistemas (MySQL/SQL dump).
 *
 * Tabelas origem: TB_ESTOQUE, TB_EST_PRODUTO, TB_UNI_MEDIDA,
 *   TB_EST_GRUPO, TB_EST_SUBGRUPO, TB_FORNECEDOR, TB_CLIENTE, TB_CLI_PF, TB_CLI_PJ.
 *
 * Ordem dos steps:
 *   1. UnidadeStep  2. NcmStep  3. CstCfopStep  4. CestStep
 *   5. CategoriaStep  6. SubcategoriaStep  7. FornecedorStep
 *   8. ProdutoStep  9. ClienteStep
 *   10. AjustePosMigracaoStep  11. AjusteGeralStep  12. GrupoTributacaoStep
 */
public class MigracaoClipp implements AdaptadorMigracao {

    private static final Logger LOG = Logger.getLogger(MigracaoClipp.class.getName());
    private static final String NOME = "MigracaoClipp";

    private final MigracaoContext ctx;
    private final MigracaoEngine.ProgressListener listener;
    private final AdaptadorMigracao.CancelCheck cancelCheck;

    public MigracaoClipp(MigracaoContext ctx,
                         MigracaoEngine.ProgressListener listener,
                         AdaptadorMigracao.CancelCheck cancelCheck) {
        this.ctx         = ctx;
        this.listener    = listener;
        this.cancelCheck = cancelCheck;
    }

    @Override public String getNome() { return NOME; }

    @Override
    public void executar(Connection conn) throws Exception {
        List<MigracaoStep> steps = buildSteps();
        int total = steps.size();
        log("[CLIPP] Iniciando " + total + " steps (Clipp Firebird → MySQL)...");

        for (int i = 0; i < total; i++) {
            if (isCancelado()) throw new MigracaoException(NOME, "Cancelado no step " + (i+1) + "/" + total);

            MigracaoStep step = steps.get(i);
            String nome = step.getNome();
            stepInicio(nome, i+1, total);
            log("[Step " + (i+1) + "/" + total + "] " + nome + " — preparando...");

            try {
                step.prepare(ctx);
                log("[Step " + (i+1) + "/" + total + "] " + nome + " — executando...");
                step.execute(ctx);
                ctx.getDestinoConn().commit();

                MigracaoContext.StepStats s = ctx.getStats().get(nome);
                int ins = s != null ? s.inseridos : 0, ign = s != null ? s.ignorados : 0, errs = s != null ? s.erros : 0;
                log("[Step " + (i+1) + "/" + total + "] " + nome + " — OK. ins=" + ins + " ign=" + ign + " err=" + errs);
                stepConcluido(nome, ins, ign, errs, true);

            } catch (MigracaoException e) {
                log("[Step " + (i+1) + "/" + total + "] " + nome + " — FALHOU: " + e.getMessage());
                try { ctx.getDestinoConn().rollback(); } catch (Exception rb) {}
                step.rollback(ctx);
                MigracaoContext.StepStats s = ctx.getStats().get(nome);
                int ins = s != null ? s.inseridos : 0, ign = s != null ? s.ignorados : 0, errs = s != null ? s.erros : 0;
                stepConcluido(nome, ins, ign, errs, false);
                step.cleanup(ctx);
                throw new MigracaoException(NOME, "Migração interrompida em '" + nome + "': " + e.getMessage(), e);
            } finally {
                step.cleanup(ctx);
            }
        }
        log("[CLIPP] Todos os steps concluídos.");
    }

    private List<MigracaoStep> buildSteps() {
        return Arrays.<MigracaoStep>asList(
            new ClippUnidadeStep(),
            new ClippNcmStep(),
            new ClippCstCfopStep(),
            new ClippCestStep(),
            new ClippCategoriaStep(),
            new ClippSubcategoriaStep(),
            new ClippFornecedorStep(),
            new ClippProdutoStep(),
            new ClippClienteStep(),
            new AjustePosMigracaoStep(),
            new AjusteGeralStep(),
            new GrupoTributacaoStep()
        );
    }

    private boolean isCancelado() { return cancelCheck != null && cancelCheck.isCancelado(); }
    private void log(String msg) { LOG.info(msg); if (listener != null) listener.onLog(msg); }
    private void stepInicio(String nome, int atual, int total) { if (listener != null) listener.onStepInicio(nome, atual, total); }
    private void stepConcluido(String n, int ins, int ign, int err, boolean ok) { if (listener != null) listener.onStepConcluido(n, ins, ign, err, ok); }
}
