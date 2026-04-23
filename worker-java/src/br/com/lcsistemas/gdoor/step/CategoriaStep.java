package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Migra grupos (categorias) do Firebird (ESTOQUE.GRUPO) para lc_sistemas.categoria.
 */
public class CategoriaStep extends StepBase {

    private static final Set<String> GENERICOS = new LinkedHashSet<>(
        java.util.Arrays.asList("GERAL", "DIVERSOS", "PADRAO", "")
    );

    @Override
    public String getNome() { return "CategoriaStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_categoria_nome ON lc_sistemas.categoria (nome)",
            "idx_categoria_nome");
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // Limpa destino (mantém id=1)
        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.categoria WHERE id > 1");
        exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.categoria AUTO_INCREMENT = 2");

        // SELECT DISTINCT GRUPO do Firebird
        Set<String> grupos = new LinkedHashSet<>();
        Statement stFb = null; ResultSet rsFb = null;
        try {
            stFb = ctx.getOrigemConn().createStatement();
            rsFb = stFb.executeQuery(
                "SELECT DISTINCT GRUPO FROM ESTOQUE WHERE GRUPO IS NOT NULL AND CHAR_LENGTH(GRUPO) >= 1");
            while (rsFb.next()) {
                String g = rsFb.getString(1);
                if (g != null && !GENERICOS.contains(g.trim().toUpperCase())) {
                    grupos.add(g.trim().toUpperCase());
                }
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao ler GRUPO do Firebird: " + e.getMessage(), e);
        } finally {
            close(rsFb); close(stFb);
        }

        // INSERT no MySQL
        int ins = 0;
        Map<String, Integer> mapaCategoria = new HashMap<>();

        if (!grupos.isEmpty()) {
            PreparedStatement ps = null;
            try {
                ps = ctx.getDestinoConn().prepareStatement(
                    "INSERT INTO lc_sistemas.categoria (nome, comissao, pode_gourmet, datahora_alteracao, ativo)" +
                    " VALUES (?, 0.000, 'SIM', NOW(), 1)",
                    Statement.RETURN_GENERATED_KEYS);
                for (String grupo : grupos) {
                    ps.setString(1, grupo);
                    ps.executeUpdate();
                    ResultSet keys = ps.getGeneratedKeys();
                    if (keys.next()) {
                        mapaCategoria.put(grupo, keys.getInt(1));
                        ins++;
                    }
                    keys.close();
                }
            } catch (SQLException e) {
                throw new MigracaoException(getNome(), "Erro ao inserir categorias: " + e.getMessage(), e);
            } finally {
                close(ps);
            }
        }

        // Também carrega categorias já existentes (id=1 padrão)
        Statement stD = null; ResultSet rsD = null;
        try {
            stD = ctx.getDestinoConn().createStatement();
            rsD = stD.executeQuery("SELECT id, nome FROM lc_sistemas.categoria");
            while (rsD.next()) mapaCategoria.put(rsD.getString(2), rsD.getInt(1));
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao recarregar categorias: " + e.getMessage(), e);
        } finally {
            close(rsD); close(stD);
        }

        contarInseridos(ctx, ins);
        ctx.setMapaCategoria(mapaCategoria);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.categoria WHERE id > 1",
            "rollback CategoriaStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DROP INDEX idx_categoria_nome ON lc_sistemas.categoria",
            "drop idx_categoria_nome");
    }
}
