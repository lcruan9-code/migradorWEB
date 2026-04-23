package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra clientes de host.clientes para lc_sistemas.cliente.
 *
 * Script base:
 *   -- Adiciona colunas auxiliares na origem
 *   ALTER TABLE host.clientes ADD COLUMN id_cidade INT(11) default 1532;
 *   ALTER TABLE host.clientes ADD COLUMN id_estado INT(11) default 16;
 *
 *   -- Resolve cidade/estado via codigo_municipio
 *   update host.clientes c
 *   inner join lc_sistemas.cidades ci on c.codigo_municipio = ci.codigocidade
 *   inner join lc_sistemas.estados es on ci.iduf = es.iduf
 *   set c.id_cidade = ci.id, c.id_estado = es.id where ci.iduf = es.iduf;
 *
 *   -- Insert
 *   insert into lc_sistemas.cliente(...) select id_cliente, cliente, raz_social, cpf_cnpj, ...
 *   from host.clientes where length(trim(cliente)) >= 1;
 *
 *   -- Expande colunas se necessário
 *   alter table lc_sistemas.cliente modify column razao_social varchar(100);
 *   alter table lc_sistemas.cliente modify column bairro varchar(100);
 */
public class ClienteStep extends StepBase {

    @Override
    public String getNome() { return "ClienteStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
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
            LOG.warning("[ClienteStep] Erro ao carregar mapa de cidades: " + e.getMessage());
        }

        // Expande colunas no destino para comportar dados do HOST
        execIgnore(destino,
            "ALTER TABLE lc_sistemas.cliente MODIFY COLUMN razao_social VARCHAR(100)",
            "expand razao_social");
        execIgnore(destino,
            "ALTER TABLE lc_sistemas.cliente MODIFY COLUMN bairro VARCHAR(100)",
            "expand bairro");

        // Limpa destino (mantém ids 1 e 2)
        exec(destino, "DELETE FROM lc_sistemas.cliente WHERE id > 2");
        // exec(destino, "ALTER TABLE lc_sistemas.cliente AUTO_INCREMENT = 1");

        String insertSql =
            "INSERT INTO lc_sistemas.cliente" +
            "  (numero_cartao, nome, razao_social, cpf_cnpj, endereco, numero, referencia, bairro, cep," +
            "   obs, pai_adi, mae_adi, email_adi, limite_credito, ie, rg," +
            "   id_cidade, id_cidade2, id_estado, id_estado2, id_pais, id_clientecanal, id_empresa)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Map<Integer, Integer> mapaCliente = new HashMap<Integer, Integer>();
        int ins = 0;

        Statement stO = null; ResultSet rsO = null;
        PreparedStatement psI = null;

        try {
            stO = origem.createStatement();
            rsO = stO.executeQuery(
                "SELECT id_cliente, cliente, raz_social, cpf_cnpj, logradouro, numero, complemento," +
                "  bairro, cep, FONE, CELULAR, CELULAR2, email, lmte_credito, ie_rg, pai, mae," +
                "  codigo_municipio" +
                " FROM CLIENTES" +
                " WHERE CLIENTE IS NOT NULL");

            psI = destino.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);

            while (rsO.next()) {
                int    idCliente  = rsO.getInt("id_cliente");
                String nome       = nvl(rsO.getString("cliente"));
                String razaoSoc   = nvl(rsO.getString("raz_social"));
                String cpfCnpj    = nvl(rsO.getString("cpf_cnpj"));
                String logradouro = nvl(rsO.getString("logradouro"));
                String numero     = nvl(rsO.getString("numero"));
                String compl      = nvl(rsO.getString("complemento"));
                String bairro     = nvl(rsO.getString("bairro"));
                String cep        = nvl(rsO.getString("cep"));
                String fone       = nvl(rsO.getString("FONE"));
                String celular    = nvl(rsO.getString("CELULAR"));
                String celular2   = nvl(rsO.getString("CELULAR2"));
                String email      = nvl(rsO.getString("email"));
                double limite     = rsO.getDouble("lmte_credito");
                String ieRg       = nvl(rsO.getString("ie_rg"));
                String pai        = nvl(rsO.getString("pai"));
                String mae        = nvl(rsO.getString("mae"));
                String codMun    = nvl(rsO.getString("codigo_municipio"));
                
                int idCidade = ctx.getConfig().getCidadeDefaultId();
                int idEstado = ctx.getConfig().getEstadoDefaultId();
                
                if (mapaCidades.containsKey(codMun)) {
                    int[] cidEst = mapaCidades.get(codMun);
                    idCidade = cidEst[0];
                    idEstado = cidEst[1];
                }

                String razaoFinal = razaoSoc.isEmpty() ? nome : razaoSoc;
                // Separar IE e RG baseado no tamanho do CPF/CNPJ
                String ie = cpfCnpj.replaceAll("[^0-9]","").length() == 11 ? "" : ieRg;
                String rg = cpfCnpj.replaceAll("[^0-9]","").length() == 11 ? ieRg : "";

                String obs = "FONE: " + fone + "\nCELULAR: " + celular +
                             "\nCELULAR 2: " + celular2 + "\n\nMIGRACAO HOST";

                psI.setInt(1,      idCliente);
                psI.setString(2,   nome);
                psI.setString(3,   razaoFinal);
                psI.setString(4,   cpfCnpj);
                psI.setString(5,   logradouro);
                psI.setString(6,   numero);
                psI.setString(7,   compl);
                psI.setString(8,   bairro);
                psI.setString(9,   cep);
                psI.setString(10,  obs);
                psI.setString(11,  pai);
                psI.setString(12,  mae);
                psI.setString(13,  email);
                psI.setDouble(14,  limite);
                psI.setString(15,  ie);
                psI.setString(16,  rg);
                psI.setInt(17,     idCidade);
                psI.setInt(18,     idCidade);
                psI.setInt(19,     idEstado);
                psI.setInt(20,     idEstado);
                psI.setInt(21,     ID_PAIS);
                psI.setInt(22,     0); // id_clientecanal
                psI.setInt(23,     ID_EMPRESA);

                psI.executeUpdate();
                ResultSet keys = psI.getGeneratedKeys();
                if (keys.next()) {
                    mapaCliente.put(idCliente, keys.getInt(1));
                    ins++;
                }
                keys.close();
            }

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar clientes: " + e.getMessage(), e);
        } finally {
            close(rsO); close(stO); close(psI);
        }

        contarInseridos(ctx, ins);
        ctx.setMapaCliente(mapaCliente);
        LOG.info("[ClienteStep] " + ins + " clientes inseridos.");
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.cliente WHERE id > 2",
            "rollback ClienteStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }
}
