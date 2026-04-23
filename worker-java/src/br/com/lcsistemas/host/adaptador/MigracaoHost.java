package br.com.lcsistemas.host.adaptador;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;
import br.com.lcsistemas.host.core.MigracaoStep;
import br.com.lcsistemas.host.engine.MigracaoEngine;
import br.com.lcsistemas.host.step.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Estratégia de migração para o sistema HOST (MySQL → MySQL).
 *
 * Ordem de execução canônica (dependências de IDs entre steps):
 *  1. NcmStep          — lc_sistemas.ncm, mapaNcm
 *  2. CestStep         — lc_sistemas.cest, mapaCest
 *  3. CategoriaStep    — lc_sistemas.categoria, mapaCategoria (usa produtos_grupo)
 *  4. FabricanteStep   — lc_sistemas.fabricante, mapaFabricante (usa produtos_marca)
 *  5. UnidadeStep      — lc_sistemas.unidade, mapaUnidade
 *  6. CstCfopStep      — atualiza id_cst/id_cfop em host.produtos
 *  7. SubcategoriaStep — placeholder (reset tabela)
 *  8. ProdutoStep      — lc_sistemas.produto (usa todos os mapas acima)
 *  9. FornecedorStep   — lc_sistemas.fornecedor, mapaFornecedor
 * 10. ClienteStep      — lc_sistemas.cliente, mapaCliente
 * 11. PagarStep        — lc_sistemas.pagar (usa mapaFornecedor via coluna codigo)
 * 12. ReceberStep      — lc_sistemas.receber (usa mapaCliente via numero_cartao)
 */
public class MigracaoHost implements AdaptadorMigracao {

    private static final Logger LOG = Logger.getLogger(MigracaoHost.class.getName());
    private static final String NOME = "MigracaoHost";

    private final MigracaoContext ctx;
    private final MigracaoEngine.ProgressListener listener;
    private final CancelCheck cancelCheck;

    public interface CancelCheck {
        boolean isCancelado();
    }

    public MigracaoHost(MigracaoContext ctx,
                        MigracaoEngine.ProgressListener listener,
                        CancelCheck cancelCheck) {
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
        log("[Host] Iniciando " + total + " steps...");

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

            } catch (MigracaoException e) {
                log("[Step " + (i + 1) + "/" + total + "] " + nome + " — FALHOU: " + e.getMessage());
                try { ctx.getDestinoConn().rollback(); } catch (Exception rb) { /* ignora */ }
                step.rollback(ctx);

                MigracaoContext.StepStats s = ctx.getStats().get(nome);
                int ins = s != null ? s.inseridos : 0;
                int ign = s != null ? s.ignorados : 0;
                int err = s != null ? s.erros     : 0;
                stepConcluido(nome, ins, ign, err, false);

                throw new MigracaoException(NOME,
                    "Migração interrompida em '" + nome + "': " + e.getMessage(), e);

            } finally {
                step.cleanup(ctx);
            }
        }

        log("[Host] Todos os steps concluídos.");
    }

    private List<MigracaoStep> buildSteps() {
        return Arrays.<MigracaoStep>asList(
            new NcmStep(),
            new CestStep(),
            new CategoriaStep(),
            new FabricanteStep(),
            new UnidadeStep(),
            new CstCfopStep(),
            new SubcategoriaStep(),
            new ProdutoStep(),
            new FornecedorStep(),
            new ClienteStep(),
            new PagarStep(),
            new ReceberStep(),
            new AjusteGeralStep(),
            new GrupoTributacaoStep()
        );
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
