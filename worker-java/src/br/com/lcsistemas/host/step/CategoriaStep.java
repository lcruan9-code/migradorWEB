package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra grupos de produtos de host.produtos_grupo para lc_sistemas.categoria.
 *
 * Script base:
 *   insert into lc_sistemas.categoria(nome, comissao, pode_gourmet, datahora_alteracao, ativo)
 *   select grupo, 0.000, 'SIM', now(), 1 from host.produtos_grupo
 *   where length(trim(grupo)) >= 1 group by grupo;
 *
 *   update host.produtos p
 *   inner join host.produtos_grupo g on p.grupo = g.id
 *   inner join lc_sistemas.categoria c on g.grupo = c.nome
 *   set p.id_categoria = c.id;
 */
public class CategoriaStep extends StepBase {

    @Override
    public String getNome() { return "CategoriaStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        // Sem colunas temporarias no Firebird
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        // Limpa destino (mantém id=1)
        exec(destino, "DELETE FROM lc_sistemas.categoria WHERE id > 1");
        // exec(destino, "ALTER TABLE lc_sistemas.categoria AUTO_INCREMENT = 1");

        Map<String, Integer> mapaCategoria = new HashMap<String, Integer>();

        PreparedStatement ps = null;
        Statement         st = null;
        ResultSet         rs = null;

        try {
            ps = destino.prepareStatement(
                "INSERT INTO lc_sistemas.categoria (nome, comissao, pode_gourmet, datahora_alteracao, ativo)" +
                " VALUES (?, 0.000, 'SIM', NOW(), 1)",
                Statement.RETURN_GENERATED_KEYS);

            st = origem.createStatement();
            rs = st.executeQuery(
                "SELECT DISTINCT GRUPO FROM PRODUTOS_GRUPO WHERE GRUPO IS NOT NULL");

            int ins = 0;
            while (rs.next()) {
                String grupo = rs.getString(1);
                if (grupo == null || grupo.trim().isEmpty()) continue;
                ps.setString(1, grupo.trim());
                ps.executeUpdate();
                ResultSet keys = ps.getGeneratedKeys();
                if (keys.next()) {
                    mapaCategoria.put(grupo.trim(), keys.getInt(1));
                    ins++;
                }
                keys.close();
            }
            contarInseridos(ctx, ins);
            LOG.info("[CategoriaStep] " + ins + " categorias inseridas.");

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar categorias: " + e.getMessage(), e);
        } finally {
            close(rs); close(st); close(ps);
        }

        // Recarrega mapa completo
        try {
            Statement stD = destino.createStatement();
            ResultSet rsD = stD.executeQuery("SELECT id, nome FROM lc_sistemas.categoria");
            while (rsD.next()) mapaCategoria.put(rsD.getString(2), rsD.getInt(1));
            rsD.close(); stD.close();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao recarregar categorias: " + e.getMessage(), e);
        }

        ctx.setMapaCategoria(mapaCategoria);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.categoria WHERE id > 1",
            "rollback CategoriaStep");
    }
}
