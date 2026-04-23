package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

/**
 * Fabricante no GDOOR não tem tabela própria no destino.
 * O campo cod_fabricante do estoque vai para o campo 'referencia' do produto.
 * Este step é um no-op: o ProdutoStep usa cod_fabricante diretamente.
 *
 * Mantido na lista de steps para consistência e futura expansão.
 */
public class FabricanteStep extends StepBase {

    @Override
    public String getNome() { return "FabricanteStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // Nenhuma ação: cod_fabricante é usado diretamente em ProdutoStep
        // como campo 'referencia' quando barras > 14 dígitos
        contarIgnorados(ctx, 0);
    }
}
