package br.com.lcsistemas.syspdv.adaptador;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.engine.MigracaoEngine;
import br.com.lcsistemas.syspdv.versao.VersaoFirebird;

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
     * @param versao      Versão detectada pelo {@link br.com.lcsistemas.syspdv.versao.DetectorFirebird}.
     * @param ctx         Contexto com conexões e configuração já montados.
     * @param listener    Listener de progresso para a UI (pode ser nulo).
     * @param cancelCheck Supplier para verificar cancelamento.
     * @return Lista imutável e ordenada de adaptadores.
     */
    public static List<AdaptadorMigracao> getMigradores(
            VersaoFirebird versao,
            MigracaoContext ctx,
            MigracaoEngine.ProgressListener listener,
            AdaptadorMigracao.CancelCheck cancelCheck) {

        String sistema = ctx.getConfig().getSistema();
        LOG.info("[FabricaMigracao] Montando estratégias para: " + versao + " | sistema=" + sistema);

        // Roteia pelo sistema de origem configurado
        if ("clipp".equals(sistema)) {
            return Arrays.<AdaptadorMigracao>asList(
                new MigracaoClipp(ctx, listener, cancelCheck)
            );
        }

        // Fallback: SYSPDV e demais sistemas Firebird
        switch (versao) {
            case FB25:
            case FB30:
            case FB40:
            case FB50:
            case DESCONHECIDA:
            default:
                return Arrays.<AdaptadorMigracao>asList(
                    new MigracaoSyspdv(ctx, listener, cancelCheck)
                );
        }
    }
}
