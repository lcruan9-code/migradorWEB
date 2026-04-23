package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra fornecedores de host.fornecedor para lc_sistemas.fornecedor.
 *
 * Script base:
 *   -- Coluna auxiliar no destino para guardar o código de origem
 *   alter table lc_sistemas.fornecedor add column codigo varchar(15) after id;
 *
 *   -- Adiciona colunas auxiliares na origem
 *   ALTER TABLE host.fornecedor ADD COLUMN id_cidade INT(11) default 1532;
 *   ALTER TABLE host.fornecedor ADD COLUMN id_estado INT(11) default 16;
 *
 *   -- Resolve cidade/estado via codigo_municipio
 *   update host.fornecedor c
 *   inner join lc_sistemas.cidades ci on c.codigo_municipio = ci.codigocidade
 *   inner join lc_sistemas.estados es on ci.iduf = es.iduf
 *   set c.id_cidade = ci.id, c.id_estado = es.id where ci.iduf = es.iduf;
 *
 *   -- Insert
 *   insert into lc_sistemas.fornecedor(codigo, nome, razao_social, cep, obs, cnpj_cpf, ie, endereco, bairro, numero, email_site, id_cidade, id_estado, id_empresa)
 *   select id_fornecedor, if(fantasia...)...
 *
 *   -- Remove coluna auxiliar ao final
 *   alter table lc_sistemas.fornecedor drop column codigo;
 */
public class FornecedorStep extends StepBase {

    @Override
    public String getNome() { return "FornecedorStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        // Coluna auxiliar (codigo = id_fornecedor host) no destino
        execIgnore(ctx.getDestinoConn(),
            "ALTER TABLE lc_sistemas.fornecedor ADD COLUMN codigo VARCHAR(15) AFTER id",
            "add codigo fornecedor");
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        Map<String, int[]> mapaCidades = new HashMap<String, int[]>();
        try {
            Statement stD = destino.createStatement();
            ResultSet rsD = stD.executeQuery(
                "SELECT ci.codigocidade, ci.id, es.id " +
                "FROM lc_sistemas.cidades ci INNER JOIN lc_sistemas.estados es ON ci.iduf = es.iduf");
            while (rsD.next()) {
                mapaCidades.put(rsD.getString(1), new int[] { rsD.getInt(2), rsD.getInt(3) });
            }
            rsD.close(); stD.close();
        } catch (SQLException e) {
            LOG.warning("[FornecedorStep] Erro ao carregar mapa de cidades: " + e.getMessage());
        }

        // Limpa destino (mantém ids 1 e 2)
        exec(destino, "DELETE FROM lc_sistemas.fornecedor WHERE id > 2");
        exec(destino, "ALTER TABLE lc_sistemas.fornecedor AUTO_INCREMENT = 3");

        String insertSql =
            "INSERT INTO lc_sistemas.fornecedor" +
            "  (codigo, nome, razao_social, cep, obs, cnpj_cpf, ie, endereco, bairro, numero, email_site," +
            "   id_cidade, id_estado, id_empresa)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Map<Integer, Integer> mapaFornecedor = new HashMap<Integer, Integer>();
        int ins = 0;

        Statement stO = null; ResultSet rsO = null;
        PreparedStatement psI = null;

        try {
            stO = origem.createStatement();
            rsO = stO.executeQuery(
                "SELECT id_fornecedor, fantasia, raz_social, cep, FONE, FAX, CELULAR," +
                "  cnpj, ie, endereco, bairro, numero, email, codigo_municipio" +
                " FROM FORNECEDOR" +
                " WHERE (FANTASIA IS NOT NULL AND TRIM(FANTASIA) <> '') OR (RAZ_SOCIAL IS NOT NULL AND TRIM(RAZ_SOCIAL) <> '')");

            psI = destino.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);

            while (rsO.next()) {
                int    idForn    = rsO.getInt("id_fornecedor");
                String fantasia  = nvl(rsO.getString("fantasia"));
                String razSoc    = nvl(rsO.getString("raz_social"));
                String cep       = nvl(rsO.getString("cep"));
                String fone      = nvl(rsO.getString("FONE"));
                String fax       = nvl(rsO.getString("FAX"));
                String celular   = nvl(rsO.getString("CELULAR"));
                String cnpj      = nvl(rsO.getString("cnpj"));
                String ie        = nvl(rsO.getString("ie"));
                String endereco  = nvl(rsO.getString("endereco"));
                String bairro    = nvl(rsO.getString("bairro"));
                String numero    = nvl(rsO.getString("numero"));
                String email     = nvl(rsO.getString("email"));
                String codMun    = nvl(rsO.getString("codigo_municipio"));
                
                int idCidade = ctx.getConfig().getCidadeDefaultId();
                int idEstado = ctx.getConfig().getEstadoDefaultId();
                
                if (mapaCidades.containsKey(codMun)) {
                    int[] cidEst = mapaCidades.get(codMun);
                    idCidade = cidEst[0];
                    idEstado = cidEst[1];
                }

                String nome  = fantasia.isEmpty() ? razSoc : fantasia;
                String razao = razSoc.isEmpty() ? fantasia : razSoc;
                String obs   = "FONE: " + fone + "\nFAX: " + fax + "\nCELULAR: " + celular;

                psI.setString(1,  String.valueOf(idForn));
                psI.setString(2,  nome);
                psI.setString(3,  razao);
                psI.setString(4,  cep);
                psI.setString(5,  obs);
                psI.setString(6,  cnpj);
                psI.setString(7,  ie);
                psI.setString(8,  endereco);
                psI.setString(9,  bairro);
                psI.setString(10, numero);
                psI.setString(11, email);
                psI.setInt(12,    idCidade);
                psI.setInt(13,    idEstado);
                psI.setInt(14,    ID_EMPRESA);

                psI.executeUpdate();
                ResultSet keys = psI.getGeneratedKeys();
                if (keys.next()) {
                    mapaFornecedor.put(idForn, keys.getInt(1));
                    ins++;
                }
                keys.close();
            }

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar fornecedores: " + e.getMessage(), e);
        } finally {
            close(rsO); close(stO); close(psI);
        }

        contarInseridos(ctx, ins);
        ctx.setMapaFornecedor(mapaFornecedor);
        LOG.info("[FornecedorStep] " + ins + " fornecedores inseridos.");
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.fornecedor WHERE id > 2",
            "rollback FornecedorStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        // Remove coluna auxiliar do destino (conforme script)
        execIgnore(ctx.getDestinoConn(),
            "ALTER TABLE lc_sistemas.fornecedor DROP COLUMN codigo",
            "drop codigo fornecedor");
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }
}
