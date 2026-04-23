package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra clientes do Firebird (CLIENTE) para lc_sistemas.cliente.
 *
 * Estratégia JDBC bidirecional:
 *   1. Carrega mapa cidades e estados do MySQL em memória
 *   2. SELECT * FROM CLIENTE no Firebird
 *   3. Para cada cliente, resolve cidade/estado via mapa Java
 *   4. INSERT no MySQL via PreparedStatement
 *   5. Monta mapaCliente (codigo_gdoor -> id_mysql) para ReceberStep
 */
public class ClienteStep extends StepBase {

    @Override
    public String getNome() { return "ClienteStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        int cidadeId = ctx.getConfig().getCidadeDefaultId();
        int estadoId = ctx.getConfig().getEstadoDefaultId();

        // 1. Carrega mapa cidades: "NOME_NORMALIZADO|UF" -> id
        //    Normaliza acentos para garantir match com nomes vindos do GDoor
        Map<String, Integer> mapaCidades = new HashMap<>();
        Map<String, Integer> mapaEstados = new HashMap<>();
        Statement stD = null; ResultSet rsD = null;
        try {
            stD = ctx.getDestinoConn().createStatement();
            rsD = stD.executeQuery(
                "SELECT c.id, c.nome, e.uf FROM lc_sistemas.cidades c " +
                "INNER JOIN lc_sistemas.estados e ON e.iduf = c.iduf");
            while (rsD.next()) {
                // Normaliza o nome da cidade (lc_sistemas) para chave sem acento
                String nomeCidade = normalizarCidade(rsD.getString(2));
                String ufCidade   = rsD.getString(3).toUpperCase().trim();
                mapaCidades.put(nomeCidade + "|" + ufCidade, rsD.getInt(1));
            }
            rsD.close();
            // Usa 'id' (PK auto-increment, mesmo valor que est.id do combo da UI),
            // NÃO 'iduf' (código IBGE: 11-53 com lacunas), para manter consistência
            // com estadoDefaultId = est.id configurado na tela do programa.
            rsD = stD.executeQuery("SELECT id, uf FROM lc_sistemas.estados");
            while (rsD.next()) mapaEstados.put(rsD.getString(2).toUpperCase().trim(), rsD.getInt(1));
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao carregar cidades/estados: " + e.getMessage(), e);
        } finally {
            close(rsD); close(stD);
        }

        // 2. Limpa destino
        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.cliente WHERE id > 2");
        exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.cliente AUTO_INCREMENT = 3");

        String insertSql =
            "INSERT INTO lc_sistemas.cliente " +
            "  (numero_cartao, ativo, id_clientecanal, nome, razao_social, apelido_adi," +
            "   cpf_cnpj, ie, rg, im, endereco, numero, referencia, bairro, cep," +
            "   telefone, tel_comercial, obs, email_adi, limite_credito," +
            "   id_cidade, id_cidade2, id_estado, id_estado2, id_pais, id_empresa) " +
            "VALUES (?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Map<Integer, Integer> mapaCliente = new HashMap<>();
        int ins = 0;

        Statement stFb = null; ResultSet rsFb = null;
        PreparedStatement psIns = null;
        try {
            stFb  = ctx.getOrigemConn().createStatement();
            rsFb  = stFb.executeQuery(
                "SELECT CODIGO, NOME, FANTASIA, CNPJ_CNPF, IE_RG, IM," +
                "       ENDERECO, NUMERO, COMPLEMENTO, BAIRRO, CEP," +
                "       TELEFONE, CELULAR, FAX, EMAIL, LIMITE_CREDITO," +
                "       CIDADE, UF, SITUACAO, CONTATO " +
                "FROM CLIENTE WHERE CODIGO > 0 AND NOME IS NOT NULL AND CHAR_LENGTH(NOME) >= 1");

            psIns = ctx.getDestinoConn().prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);

            while (rsFb.next()) {
                int    codigo    = rsFb.getInt("CODIGO");
                String nome      = nvl(rsFb.getString("NOME"));
                String fantasia  = nvl(rsFb.getString("FANTASIA"));
                String cnpj      = nvl(rsFb.getString("CNPJ_CNPF"));
                String ie        = nvl(rsFb.getString("IE_RG"));
                String im        = nvl(rsFb.getString("IM"));
                String endereco  = nvl(rsFb.getString("ENDERECO")).toUpperCase();
                String numero    = nvl(rsFb.getString("NUMERO"));
                String compl     = nvl(rsFb.getString("COMPLEMENTO")).toUpperCase();
                String bairro    = nvl(rsFb.getString("BAIRRO")).toUpperCase();
                String cep       = nvl(rsFb.getString("CEP"));
                String tel       = limparTelefone(nvl(rsFb.getString("TELEFONE")), 20);
                String cel       = limparTelefone(nvl(rsFb.getString("CELULAR")), 20);
                String fax       = nvl(rsFb.getString("FAX"));
                String email     = nvl(rsFb.getString("EMAIL"));
                double limite    = rsFb.getDouble("LIMITE_CREDITO");
                String cidade    = nvl(rsFb.getString("CIDADE")).toUpperCase().trim();
                String uf        = nvl(rsFb.getString("UF")).toUpperCase().trim();
                String situacao  = nvl(rsFb.getString("SITUACAO"));
                String contato   = nvl(rsFb.getString("CONTATO"));
                if (contato.length() > 20) contato = contato.substring(0, 20);

                // Se nome vazio, usa fantasia
                if (nome.isEmpty()) nome = fantasia;

                String rg        = cnpj.length() == 14 ? ie : "";
                String ieIns     = cnpj.length() > 14  ? ie : "";

                // Normaliza o nome vindo do GDoor antes de buscar no mapa
                String key = normalizarCidade(cidade) + "|" + uf;
                int idCidade = mapaCidades.getOrDefault(key, cidadeId);
                int idEstado = mapaEstados.getOrDefault(uf, estadoId);

                String obs = "TELEFONE: " + tel + "\nCELULAR: " + cel + "\nFAX: " + fax;

                psIns.setInt(1,     codigo);
                psIns.setInt(2,     situacao.equalsIgnoreCase("Ativo") ? 1 : 0);
                psIns.setString(3,  nome);
                psIns.setString(4,  cnpj.length() >= 18 ? nome : "");
                psIns.setString(5,  contato);
                psIns.setString(6,  cnpj);
                psIns.setString(7,  ieIns);
                psIns.setString(8,  rg);
                psIns.setString(9,  im);
                psIns.setString(10, endereco);
                psIns.setString(11, numero);
                psIns.setString(12, compl);
                psIns.setString(13, bairro);
                psIns.setString(14, cep);
                psIns.setString(15, tel);
                psIns.setString(16, cel);
                psIns.setString(17, obs);
                psIns.setString(18, email);
                psIns.setDouble(19, limite);
                psIns.setInt(20,    idCidade);
                psIns.setInt(21,    idCidade);
                psIns.setInt(22,    idEstado);
                psIns.setInt(23,    idEstado);
                psIns.setInt(24,    ID_PAIS);
                psIns.setInt(25,    ID_EMPRESA);

                psIns.executeUpdate();
                ResultSet keys = psIns.getGeneratedKeys();
                if (keys.next()) {
                    mapaCliente.put(codigo, keys.getInt(1));
                    ins++;
                }
                keys.close();
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar clientes: " + e.getMessage(), e);
        } finally {
            close(rsFb); close(stFb); close(psIns);
        }

        contarInseridos(ctx, ins);
        ctx.setMapaCliente(mapaCliente);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.cliente WHERE id > 2", "rollback ClienteStep");
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }

    /**
     * Normaliza nome de cidade para lookup sem acento:
     *   UPPER + remove acentos comuns (ISO-8859-1 / UTF-8 mojibake).
     * Garante que "São Paulo" do GDoor bata com "SAO PAULO" do lc_sistemas.
     */
    private String normalizarCidade(String nome) {
        if (nome == null) return "";
        String n = nome.toUpperCase().trim();
        n = n.replace("\u00C0","A").replace("\u00C1","A").replace("\u00C2","A").replace("\u00C3","A").replace("\u00C4","A");
        n = n.replace("\u00C8","E").replace("\u00C9","E").replace("\u00CA","E").replace("\u00CB","E");
        n = n.replace("\u00CC","I").replace("\u00CD","I").replace("\u00CE","I").replace("\u00CF","I");
        n = n.replace("\u00D2","O").replace("\u00D3","O").replace("\u00D4","O").replace("\u00D5","O").replace("\u00D6","O");
        n = n.replace("\u00D9","U").replace("\u00DA","U").replace("\u00DB","U").replace("\u00DC","U");
        n = n.replace("\u00C7","C").replace("\u00D1","N");
        // mojibake UTF-8/Latin-1
        n = n.replace("\u00C3\u00A3","A").replace("\u00C3\u00A2","A").replace("\u00C3\u00A0","A").replace("\u00C3\u00A1","A");
        n = n.replace("\u00C3\u00A9","E").replace("\u00C3\u00AA","E");
        n = n.replace("\u00C3\u00AD","I");
        n = n.replace("\u00C3\u00B3","O").replace("\u00C3\u00B4","O").replace("\u00C3\u00B5","O");
        n = n.replace("\u00C3\u00BA","U");
        n = n.replace("\u00C3\u00A7","C");
        return n.trim();
    }
}
