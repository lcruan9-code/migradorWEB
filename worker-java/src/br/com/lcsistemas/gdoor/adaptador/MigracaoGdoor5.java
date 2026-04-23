package br.com.lcsistemas.gdoor.adaptador;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;
import br.com.lcsistemas.gdoor.core.MigracaoStep;
import br.com.lcsistemas.gdoor.engine.MigracaoEngine;
import br.com.lcsistemas.gdoor.step.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Estratégia de migração para Gdoor 5.x — PRODUÇÃO, VALIDADA.
 *
 * <p><b>REGRA CRÍTICA:</b> Esta classe encapsula EXATAMENTE a lógica de migração
 * que estava em {@code MigracaoEngine.migrar()} (steps, índices Firebird, commit).
 * Nenhuma regra de negócio foi alterada — apenas movida para cá.
 *
 * <p>O contexto ({@link MigracaoContext}) contém as duas conexões JDBC
 * (origem Firebird + destino MySQL) já abertas pelo {@code MigracaoEngine}.
 * O parâmetro {@code conn} de {@link #executar(Connection)} é a conexão de origem.
 *
 * <p>Esta classe é instanciada pela {@link FabricaMigracao} e executada
 * pelo {@link ExecutorMigracao}.
 */
public class MigracaoGdoor5 implements AdaptadorMigracao {

    private static final Logger LOG = Logger.getLogger(MigracaoGdoor5.class.getName());

    private static final String NOME = "MigracaoGdoor5";

    /** Contexto compartilhado com ambas as conexões e configurações. */
    private final MigracaoContext ctx;

    /** Listener de progresso para a UI — pode ser nulo (safe). */
    private final MigracaoEngine.ProgressListener listener;

    /** Supplier de cancelamento — checado entre steps. */
    private final CancelCheck cancelCheck;

    /**
     * Interface funcional simples para checar cancelamento.
     * Compatível com Java 8+.
     */
    public interface CancelCheck {
        boolean isCancelado();
    }

    /**
     * Constrói o adaptador Gdoor 5.x.
     *
     * @param ctx         Contexto com conexões e configuração já montados.
     * @param listener    Listener de progresso para a UI (pode ser nulo).
     * @param cancelCheck Supplier para verificar se a operação foi cancelada.
     */
    public MigracaoGdoor5(MigracaoContext ctx,
                          MigracaoEngine.ProgressListener listener,
                          CancelCheck cancelCheck) {
        this.ctx         = ctx;
        this.listener    = listener;
        this.cancelCheck = cancelCheck;
    }

    @Override
    public String getNome() {
        return NOME;
    }

    /**
     * Executa a migração Gdoor 5.x completa.
     *
     * <p>Ordem de execução (imutável — definida pela SKILL ARQUITETURA_ETL):
     * <ol>
     *   <li>Criar índices temporários no Firebird (performance)</li>
     *   <li>Executar os 15 steps em sequência</li>
     *   <li>Remover índices temporários</li>
     * </ol>
     *
     * @param conn Conexão de origem (Firebird) — mesma que {@code ctx.getOrigemConn()}.
     * @throws Exception em caso de falha irrecuperável em algum step.
     */
    @Override
    public void executar(Connection conn) throws Exception {

        // ── Índices temporários no Firebird ───────────────────────────────────
        log("[Gdoor5] Criando índices temporários no Firebird (GDOOR)...");
        criarIndicesTemporarios(ctx);
        log("[Gdoor5] Índices criados.");

        if (isCancelado()) throw new MigracaoException(NOME, "Cancelado antes dos steps.");

        // ── Executar steps na ordem canônica (ARQUITETURA_ETL) ───────────────
        List<MigracaoStep> steps = buildSteps();
        int total = steps.size();
        log("[Gdoor5] Iniciando " + total + " steps...");

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
                log("[Step " + (i + 1) + "/" + total + "] " + nome
                    + " — FALHOU: " + e.getMessage());
                try { ctx.getDestinoConn().rollback(); } catch (Exception rb) { /* ignora */ }
                step.rollback(ctx);

                MigracaoContext.StepStats s = ctx.getStats().get(nome);
                int ins = s != null ? s.inseridos : 0;
                int ign = s != null ? s.ignorados : 0;
                int err = s != null ? s.erros     : 0;
                stepConcluido(nome, ins, ign, err, false);
                step.cleanup(ctx);

                // Propaga para o ExecutorMigracao acionar o fallback (se houver)
                throw new MigracaoException(NOME,
                    "Migração interrompida em '" + nome + "': " + e.getMessage(), e);

            } finally {
                step.cleanup(ctx);
            }
        }

        // ── Remover índices temporários ───────────────────────────────────────
        removerIndicesTemporarios(ctx);

        // Resumo exibido pelo MigracaoEngine após retorno do ExecutorMigracao.
        log("[Gdoor5] Todos os steps concluídos. Controle retorna ao MigracaoEngine.");
    }

    // ── Lista de steps — EXATAMENTE igual ao MigracaoEngine.buildSteps() ─────
    // REGRA: NÃO alterar ordem. Ordem definida pela SKILL ARQUITETURA_ETL.

    private List<MigracaoStep> buildSteps() {
        return Arrays.<MigracaoStep>asList(
            new NcmStep(),
            new UnidadeStep(),
            new CestStep(),
            new CategoriaStep(),
            new SubcategoriaStep(),
            new CstCfopStep(),
            new FabricanteStep(),
            new FornecedorStep(),
            new ProdutoStep(),
            new ClienteStep(),
            new PagarStep(),
            new ReceberStep(),
            new AjustePosMigracaoStep(),
            new AjusteGeralStep(),       // OBRIGATÓRIO antes do GrupoTributacao
            new GrupoTributacaoStep()    // SEMPRE por último
        );
    }

    // ── Índices temporários no Firebird ───────────────────────────────────────
    // REGRA: Copiado EXATAMENTE do MigracaoEngine. Não alterar.

    private void criarIndicesTemporarios(MigracaoContext ctx) {
        String[] idxs = {
            "CREATE INDEX IDX_EST_CODNCM  ON ESTOQUE  (COD_NCM)",
            "CREATE INDEX IDX_EST_FORN    ON ESTOQUE  (FORNECEDOR)",
            "CREATE INDEX IDX_EST_UND     ON ESTOQUE  (UND)",
            "CREATE INDEX IDX_EST_CEST    ON ESTOQUE  (COD_CEST)",
            "CREATE INDEX IDX_EST_GRUPO   ON ESTOQUE  (GRUPO)",
            "CREATE INDEX IDX_EST_ST      ON ESTOQUE  (ST)",
            "CREATE INDEX IDX_FORN_CIDADE ON FORNECEDOR (CIDADE)",
            "CREATE INDEX IDX_FORN_UF     ON FORNECEDOR (UF)",
            "CREATE INDEX IDX_CLI_CIDADE  ON CLIENTE  (CIDADE)",
            "CREATE INDEX IDX_CLI_UF      ON CLIENTE  (UF)"
        };
        executarMelhorEsforco(ctx.getOrigemConn(), idxs, "criação de índices Firebird");
    }

    private void removerIndicesTemporarios(MigracaoContext ctx) {
        // No Firebird DROP INDEX não aceita ON tabela
        String[] drops = {
            "DROP INDEX IDX_EST_CODNCM",
            "DROP INDEX IDX_EST_FORN",
            "DROP INDEX IDX_EST_UND",
            "DROP INDEX IDX_EST_CEST",
            "DROP INDEX IDX_EST_GRUPO",
            "DROP INDEX IDX_EST_ST",
            "DROP INDEX IDX_FORN_CIDADE",
            "DROP INDEX IDX_FORN_UF",
            "DROP INDEX IDX_CLI_CIDADE",
            "DROP INDEX IDX_CLI_UF"
        };
        executarMelhorEsforco(ctx.getOrigemConn(), drops, "remoção de índices Firebird");
    }

    private void executarMelhorEsforco(Connection conn, String[] sqls, String descricao) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                try { stmt.execute(sql); }
                catch (SQLException e) {
                    LOG.fine("[" + descricao + "] ignorado: " + e.getMessage());
                }
            }
        } catch (SQLException e) {
            LOG.warning("Erro ao executar " + descricao + ": " + e.getMessage());
        } finally {
            if (stmt != null) try { stmt.close(); } catch (SQLException ignored) {}
        }
    }

    // ── Helpers de cancelamento e notificação ─────────────────────────────────

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
