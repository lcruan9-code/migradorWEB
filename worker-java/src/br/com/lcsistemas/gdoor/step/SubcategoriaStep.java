package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

/**
 * No GDOOR, não existe tabela de subcategoria mapeada no destino lc_sistemas
 * com o mesmo fluxo de migração do script de referência.
 * Este step é reservado para futura expansão.
 *
 * Mantido na lista de steps do MigracaoEngine para consistência.
 */
public class SubcategoriaStep extends StepBase {

    @Override
    public String getNome() { return "SubcategoriaStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // Nenhuma ação — subcategoria não mapeada no script de referência.
        contarIgnorados(ctx, 0);
    }
}
