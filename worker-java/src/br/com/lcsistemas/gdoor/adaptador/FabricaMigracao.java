package br.com.lcsistemas.gdoor.adaptador;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.engine.MigracaoEngine;
import br.com.lcsistemas.gdoor.versao.VersaoFirebird;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Fábrica de estratégias de migração.
 *
 * <p>Recebe a {@link VersaoFirebird} detectada em runtime e retorna a lista
 * ordenada de {@link AdaptadorMigracao} a ser tentada pelo {@link ExecutorMigracao}.
 *
 * <p>A lista é ordenada por <b>prioridade de confiança</b>: o primeiro adaptador
 * é sempre o mais aderente à versão detectada. Se falhar, o executor tenta o próximo.
 *
 * <p><b>Design:</b> Para adicionar suporte a uma nova versão do Gdoor:
 * <ol>
 *   <li>Crie uma nova classe implementando {@link AdaptadorMigracao}.</li>
 *   <li>Adicione um novo {@code case} no switch abaixo.</li>
 *   <li>Nenhum outro arquivo precisa ser alterado.</li>
 * </ol>
 *
 * Mapeamento atual:
 * <pre>
 *   FB25        → [MigracaoGdoor5]
 *   FB30        → [MigracaoGdoor5]
 *   FB40        → [MigracaoGdoor5]
 *   FB50        → [MigracaoGdoor5]
 *   DESCONHECIDA → [MigracaoGdoor5]  (fallback seguro)
 * </pre>
 */
public final class FabricaMigracao {

    private static final Logger LOG = Logger.getLogger(FabricaMigracao.class.getName());

    /** Classe utilitária — não instanciar. */
    private FabricaMigracao() {}

    /**
     * Retorna a lista ordenada de adaptadores para a versão do Firebird detectada.
     *
     * @param versao      Versão detectada pelo {@link br.com.lcsistemas.gdoor.versao.DetectorFirebird}.
     * @param ctx         Contexto com conexões e configuração já montados.
     * @param listener    Listener de progresso para a UI (pode ser nulo).
     * @param cancelCheck Supplier para verificar cancelamento.
     * @return Lista imutável e ordenada de adaptadores.
     */
    public static List<AdaptadorMigracao> getMigradores(
            VersaoFirebird versao,
            MigracaoContext ctx,
            MigracaoEngine.ProgressListener listener,
            MigracaoGdoor5.CancelCheck cancelCheck) {

        LOG.info("[FabricaMigracao] Montando estratégias para: " + versao);

        switch (versao) {

            // ── Versões Firebird mapeadas ──────────────────────────────────────
            // Todas atualmente resolvidas pela MigracaoGdoor5 (validada).
            // Futuras versões do Gdoor adicionam novos cases sem alterar os existentes.

            case FB25:
                return Arrays.<AdaptadorMigracao>asList(
                    new MigracaoGdoor5(ctx, listener, cancelCheck)
                    // Futuro: new MigracaoGdoor6(ctx, listener, cancelCheck)
                );

            case FB30:
                return Arrays.<AdaptadorMigracao>asList(
                    new MigracaoGdoor5(ctx, listener, cancelCheck)
                    // Futuro: new MigracaoGdoor6(ctx, listener, cancelCheck)
                );

            case FB40:
                return Arrays.<AdaptadorMigracao>asList(
                    new MigracaoGdoor5(ctx, listener, cancelCheck)
                    // Futuro: new MigracaoGdoor6(ctx, listener, cancelCheck)
                );

            case FB50:
                return Arrays.<AdaptadorMigracao>asList(
                    new MigracaoGdoor5(ctx, listener, cancelCheck)
                    // Futuro: new MigracaoGdoor6(ctx, listener, cancelCheck)
                );

            case DESCONHECIDA:
            default:
                // Fallback seguro: tenta a estratégia mais testada
                LOG.warning("[FabricaMigracao] Versão desconhecida — usando MigracaoGdoor5 como fallback.");
                return Arrays.<AdaptadorMigracao>asList(
                    new MigracaoGdoor5(ctx, listener, cancelCheck)
                );
        }
    }
}
