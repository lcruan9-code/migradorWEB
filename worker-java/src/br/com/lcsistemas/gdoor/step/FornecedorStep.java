package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra fornecedores do Firebird (FORNECEDOR) para lc_sistemas.fornecedor.
 *
 * Estratégia:
 *   1. Carrega mapa cidades/estados do MySQL em memória (nome+uf -> id)
 *   2. SELECT * FROM FORNECEDOR no Firebird
 *   3. Para cada fornecedor, resolve cidade/estado via mapa Java
 *   4. INSERT no MySQL via PreparedStatement
 *   5. Monta mapaFornecedor (codigo_gdoor -> id_mysql) para PagarStep
 */
public class FornecedorStep extends StepBase {

    @Override
    public String getNome() { return "FornecedorStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_cidades_nome ON lc_sistemas.cidades (nome)", "idx_cid_nome");
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_estados_uf ON lc_sistemas.estados (uf)", "idx_est_uf");
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        int cidadeId = ctx.getConfig().getCidadeDefaultId();
        int estadoId = ctx.getConfig().getEstadoDefaultId();

        // 1. Carrega mapa cidades: "NOME_CIDADE|UF" -> id
        Map<String, Integer> mapaCidades = new HashMap<>();
        Map<String, Integer> mapaEstados = new HashMap<>(); // uf -> id
        Statement stD = null; ResultSet rsD = null;
        try {
            stD = ctx.getDestinoConn().createStatement();
            rsD = stD.executeQuery(
                "SELECT c.id, c.nome, e.uf FROM lc_sistemas.cidades c " +
                "INNER JOIN lc_sistemas.estados e ON e.iduf = c.iduf");
            while (rsD.next()) {
                mapaCidades.put(
                    rsD.getString(2).toUpperCase().trim() + "|" + rsD.getString(3).toUpperCase().trim(),
                    rsD.getInt(1));
            }
            rsD.close();

            rsD = stD.executeQuery("SELECT iduf, uf FROM lc_sistemas.estados");
            while (rsD.next()) mapaEstados.put(rsD.getString(2).toUpperCase().trim(), rsD.getInt(1));
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao carregar cidades/estados: " + e.getMessage(), e);
        } finally {
            close(rsD); close(stD);
        }

        // 2. Limpa destino
        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.fornecedor WHERE id > 2");
        exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.fornecedor AUTO_INCREMENT = 3");

        // 3. SELECT fornecedores do Firebird
        String insertSql =
            "INSERT INTO lc_sistemas.fornecedor " +
            "  (nome, razao_social, cnpj_cpf, ie, endereco, numero, bairro, cep," +
            "   fone, obs, id_cidade, id_estado, id_empresa) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Map<Integer, Integer> mapaFornecedor = new HashMap<>(); // codigo_gdoor -> id_mysql
        int ins = 0;

        Statement stFb = null; ResultSet rsFb = null;
        PreparedStatement psIns = null;
        try {
            stFb = ctx.getOrigemConn().createStatement();
            rsFb = stFb.executeQuery(
                "SELECT CODIGO, NOME, FANTASIA, CNPJ_CNPF, IE_RG, ENDERECO, NUMERO, BAIRRO, CEP," +
                "       TELEFONE, CELULAR, FAX, CIDADE, UF " +
                "FROM FORNECEDOR WHERE NOME IS NOT NULL AND CHAR_LENGTH(NOME) >= 1");

            psIns = ctx.getDestinoConn().prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);

            while (rsFb.next()) {
                int    codigo    = rsFb.getInt("CODIGO");
                String nome      = nvl(rsFb.getString("NOME"));
                String fantasia  = nvl(rsFb.getString("FANTASIA"));
                String cnpj      = nvl(rsFb.getString("CNPJ_CNPF"));
                String ie        = nvl(rsFb.getString("IE_RG"));
                String endereco  = nvl(rsFb.getString("ENDERECO")).toUpperCase();
                String numero    = nvl(rsFb.getString("NUMERO"));
                String bairro    = nvl(rsFb.getString("BAIRRO")).toUpperCase();
                String cep       = nvl(rsFb.getString("CEP"));
                String telefone  = limparTelefone(nvl(rsFb.getString("TELEFONE")), 20);
                String celular   = limparTelefone(nvl(rsFb.getString("CELULAR")), 20);
                String fax       = nvl(rsFb.getString("FAX"));
                String cidade    = nvl(rsFb.getString("CIDADE")).toUpperCase().trim();
                String uf        = nvl(rsFb.getString("UF")).toUpperCase().trim();

                String nomeIns = fantasia.isEmpty() ? nome : fantasia;
                String razao   = cnpj.length() == 18 ? nome : "";

                // Resolve cidade/estado
                String key = cidade + "|" + uf;
                int idCidade = mapaCidades.getOrDefault(key, cidadeId);
                int idEstado = mapaEstados.getOrDefault(uf, estadoId);

                String obs = "TELEFONE: " + telefone + "\nCELULAR: " + celular + "\nFAX: " + fax;

                psIns.setString(1,  nomeIns);
                psIns.setString(2,  razao);
                psIns.setString(3,  cnpj);
                psIns.setString(4,  ie);
                psIns.setString(5,  endereco);
                psIns.setString(6,  numero);
                psIns.setString(7,  bairro);
                psIns.setString(8,  cep);
                psIns.setString(9,  telefone);
                psIns.setString(10, obs);
                psIns.setInt(11,    idCidade);
                psIns.setInt(12,    idEstado);
                psIns.setInt(13,    ID_EMPRESA);

                psIns.executeUpdate();
                ResultSet keys = psIns.getGeneratedKeys();
                if (keys.next()) {
                    mapaFornecedor.put(codigo, keys.getInt(1));
                    ins++;
                }
                keys.close();
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar fornecedores: " + e.getMessage(), e);
        } finally {
            close(rsFb); close(stFb); close(psIns);
        }

        contarInseridos(ctx, ins);
        ctx.setMapaFornecedor(mapaFornecedor);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.fornecedor WHERE id > 2",
            "rollback FornecedorStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DROP INDEX idx_cidades_nome ON lc_sistemas.cidades", "drop idx");
        execIgnore(ctx.getDestinoConn(), "DROP INDEX idx_estados_uf   ON lc_sistemas.estados", "drop idx");
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }

}
