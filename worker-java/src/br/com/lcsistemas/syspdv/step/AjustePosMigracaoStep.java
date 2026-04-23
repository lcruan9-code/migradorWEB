package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

/**
 * Limpeza pos-migracao SYSPDV (Firebird).
 *
 * Nao ha colunas temporarias no Firebird (origem e somente leitura).
 * Limpa apenas os mapas em memoria e loga conclusao.
 */
public class AjustePosMigracaoStep extends StepBase {

    @Override
    public String getNome() { return "AjustePosMigracaoStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // Firebird e somente leitura — sem colunas temporarias para remover.
        // Limpa mapas de memoria para liberar recursos.
        ctx.put("mapaSubcategoria", null);
        ctx.put("mapaCstA", null);
        ctx.put("mapaCstB", null);
        ctx.put("mapaTributacao", null);

        LOG.info("[AjustePosMigracaoStep] Mapas de memoria liberados.");
        contarInseridos(ctx, 0);
    }
}
