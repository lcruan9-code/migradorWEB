package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

/**
 * Placeholder para subcategorias (host.produtos_subgrupo → lc_sistemas.subcategoria).
 * O script original apenas analisa a tabela mas não insere dados.
 * Este step apenas reseta a tabela de destino.
 */
public class SubcategoriaStep extends StepBase {

    @Override
    public String getNome() { return "SubcategoriaStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.subcategoria WHERE id > 1");
        // exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.subcategoria AUTO_INCREMENT = 1");
        contarInseridos(ctx, 0);
        LOG.info("[SubcategoriaStep] Subcategorias não migradas (sem dados no HOST).");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.subcategoria WHERE id > 1",
            "rollback SubcategoriaStep");
    }
}
