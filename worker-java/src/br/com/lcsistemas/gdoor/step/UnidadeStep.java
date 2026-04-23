package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra unidades de medida únicas do Firebird (ESTOQUE.UND) para lc_sistemas.unidade.
 */
public class UnidadeStep extends StepBase {

    private static final java.util.Set<String> INVALIDAS = new java.util.HashSet<>(
        java.util.Arrays.asList("1","789","1UN",".",")","%","","",",00","UNI","210",",un","UD","UN")
    );

    @Override
    public String getNome() { return "UnidadeStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_unidade_descricao ON lc_sistemas.unidade (descricao)",
            "idx_unidade_descricao ja existe");
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // 1. Coleta unidades distintas já existentes no destino
        Map<String, Integer> destMap = new HashMap<>();
        Statement stDest = null;
        ResultSet rsDest = null;
        try {
            stDest = ctx.getDestinoConn().createStatement();
            rsDest = stDest.executeQuery("SELECT id, descricao FROM lc_sistemas.unidade");
            while (rsDest.next()) destMap.put(rsDest.getString(2), rsDest.getInt(1));
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao ler unidades destino: " + e.getMessage(), e);
        } finally {
            close(rsDest); close(stDest);
        }

        // 2. SELECT DISTINCT und do Firebird
        java.util.Set<String> novas = new java.util.LinkedHashSet<>();
        Statement stFb = null;
        ResultSet rsFb = null;
        try {
            stFb = ctx.getOrigemConn().createStatement();
            rsFb = stFb.executeQuery("SELECT DISTINCT UND FROM ESTOQUE WHERE UND IS NOT NULL");
            while (rsFb.next()) {
                String und = rsFb.getString(1);
                if (und == null) continue;
                und = und.trim();
                if (und.isEmpty() || INVALIDAS.contains(und)) und = "UN";
                if (!destMap.containsKey(und)) novas.add(und);
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao ler UND do Firebird: " + e.getMessage(), e);
        } finally {
            close(rsFb); close(stFb);
        }

        // 3. INSERT novas unidades
        int ins = 0;
        if (!novas.isEmpty()) {
            PreparedStatement ps = null;
            try {
                ps = ctx.getDestinoConn().prepareStatement(
                    "INSERT INTO lc_sistemas.unidade (descricao, nome, fator_conversao, datahora_alteracao, ativo)" +
                    " VALUES (?, '', '1.000', NOW(), '1')");
                for (String und : novas) {
                    ps.setString(1, und);
                    ps.addBatch();
                    ins++;
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new MigracaoException(getNome(), "Erro ao inserir unidades: " + e.getMessage(), e);
            } finally {
                close(ps);
            }
        }

        contarInseridos(ctx, ins);

        // 4. Recarrega mapa completo para uso do ProdutoStep
        carregarMapaUnidade(ctx);
    }

    private void carregarMapaUnidade(MigracaoContext ctx) throws MigracaoException {
        Map<String, Integer> mapa = new HashMap<>();
        Statement st = null; ResultSet rs = null;
        try {
            st = ctx.getDestinoConn().createStatement();
            rs = st.executeQuery("SELECT id, descricao FROM lc_sistemas.unidade");
            while (rs.next()) mapa.put(rs.getString(2), rs.getInt(1));
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao recarregar mapa unidade: " + e.getMessage(), e);
        } finally {
            close(rs); close(st);
        }
        ctx.setMapaUnidade(mapa);
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DROP INDEX idx_unidade_descricao ON lc_sistemas.unidade",
            "drop idx_unidade_descricao");
    }
}
