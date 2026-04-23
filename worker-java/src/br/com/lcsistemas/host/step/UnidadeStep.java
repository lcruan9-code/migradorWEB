package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra unidades de medida de host.produtos para lc_sistemas.unidade.
 *
 * Script base:
 *   insert into lc_sistemas.unidade(descricao, nome, fator_conversao, datahora_alteracao, ativo)
 *   select unidade_comecial, '', IF(fator = 0, 1, fator), now(), 1 from host.produtos
 *   where length(trim(unidade_comecial)) >= 1
 *   and unidade_comecial not in(select descricao from lc_sistemas.unidade)
 *   group by unidade_comecial;
 *
 *   update host.produtos e
 *   inner join lc_sistemas.unidade u on e.unidade_comecial = u.descricao
 *   set e.id_unidade = u.id;
 */
public class UnidadeStep extends StepBase {

    @Override
    public String getNome() { return "UnidadeStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        // 1. Carrega unidades já existentes no destino (as 36 padrão)
        Map<String, Integer> mapaUnidade = new HashMap<String, Integer>();
        try {
            Statement stD = destino.createStatement();
            ResultSet rsD = stD.executeQuery("SELECT id, descricao FROM lc_sistemas.unidade");
            while (rsD.next()) mapaUnidade.put(rsD.getString(2), rsD.getInt(1));
            rsD.close(); stD.close();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao carregar unidades destino: " + e.getMessage(), e);
        }

        // 2. Reset do auto_increment acima das unidades padrão
        exec(destino, "DELETE FROM lc_sistemas.unidade WHERE id > 36");
        // exec(destino, "ALTER TABLE lc_sistemas.unidade AUTO_INCREMENT = 1");

        // 3. Coleta unidades novas da origem
        PreparedStatement ps = null;
        Statement         st = null;
        ResultSet         rs = null;
        int ins = 0;

        try {
            ps = destino.prepareStatement(
                "INSERT INTO lc_sistemas.unidade (descricao, nome, fator_conversao, datahora_alteracao, ativo)" +
                " VALUES (?, '', ?, NOW(), '1')",
                Statement.RETURN_GENERATED_KEYS);

            st = origem.createStatement();
            rs = st.executeQuery(
                "SELECT DISTINCT UNIDADE_COMECIAL, FATOR FROM PRODUTOS WHERE UNIDADE_COMECIAL IS NOT NULL");

            while (rs.next()) {
                String und   = rs.getString(1);
                double fator = rs.getDouble(2);
                if (und == null || und.trim().isEmpty()) continue;
                und = und.trim();
                if (mapaUnidade.containsKey(und)) continue; // já existe

                ps.setString(1, und);
                ps.setDouble(2, fator == 0 ? 1.0 : fator);
                ps.executeUpdate();
                ResultSet keys = ps.getGeneratedKeys();
                if (keys.next()) {
                    mapaUnidade.put(und, keys.getInt(1));
                    ins++;
                }
                keys.close();
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao inserir unidades: " + e.getMessage(), e);
        } finally {
            close(rs); close(st); close(ps);
        }

        contarInseridos(ctx, ins);
        LOG.info("[UnidadeStep] " + ins + " unidades novas inseridas.");

        // 4. Recarrega mapa completo
        try {
            Statement stD = destino.createStatement();
            ResultSet rsD = stD.executeQuery("SELECT id, descricao FROM lc_sistemas.unidade");
            while (rsD.next()) mapaUnidade.put(rsD.getString(2), rsD.getInt(1));
            rsD.close(); stD.close();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao recarregar unidades: " + e.getMessage(), e);
        }

        ctx.setMapaUnidade(mapaUnidade);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.unidade WHERE id > 36",
            "rollback UnidadeStep");
    }
}
