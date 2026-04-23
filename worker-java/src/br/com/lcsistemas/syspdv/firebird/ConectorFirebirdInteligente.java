package br.com.lcsistemas.syspdv.firebird;

import br.com.lcsistemas.syspdv.adaptador.ExecutorMigracao.LogCallback;
import br.com.lcsistemas.syspdv.config.MigracaoConfig;
import br.com.lcsistemas.syspdv.versao.VersaoFirebird;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class ConectorFirebirdInteligente {

    public static Connection conectar(MigracaoConfig config, LogCallback log) {

        File pastaFb = GerenciadorFirebird.resolverPasta(config);
        List<FirebirdInstalacao> instalacoes = GerenciadorFirebird.descobrirInstalacoes(pastaFb);

        if (instalacoes.isEmpty()) {
            throw new RuntimeException("Nenhuma instalação do Firebird encontrada.");
        }

        log.log("🔍 Iniciando varredura de versões do Firebird...");

        for (FirebirdInstalacao inst : instalacoes) {

            log.log("\n➡ Tentando versão: " + inst.getNomePasta());

            boolean iniciou = GerenciadorFirebird.iniciarInstalacao(inst, (GerenciadorFirebird.LogCallback) log);

            if (!iniciou) {
                log.log("⚠ Não conseguiu iniciar essa versão.");
                continue;
            }

            try {
                Connection conn = tentarConexao(config, log);
                log.log("✅ SUCESSO: Conectado com " + inst.getVersaoStr());
                return conn;

            } catch (SQLException ex) {

                String erro = ex.getMessage();
                log.log("❌ Falha ao conectar: " + erro);

                // 🔥 Detecta ODS automaticamente
                VersaoFirebird versaoODS = GerenciadorFirebird.detectarVersaoPorODS(erro);

                if (versaoODS != VersaoFirebird.DESCONHECIDA) {

                    log.log("🧠 ODS detectado → precisa da versão: " + versaoODS);

                    GerenciadorFirebird.pararSeIniciado((GerenciadorFirebird.LogCallback) log);

                    boolean trocou = GerenciadorFirebird.trocarParaVersao(versaoODS, config, (GerenciadorFirebird.LogCallback) log);

                    if (trocou) {
                        try {
                            Connection conn = tentarConexao(config, log);
                            log.log("✅ SUCESSO após ajuste por ODS!");
                            return conn;
                        } catch (SQLException ex2) {
                            log.log("❌ Ainda falhou após ajuste ODS: " + ex2.getMessage());
                        }
                    }
                }

                GerenciadorFirebird.pararSeIniciado((GerenciadorFirebird.LogCallback) log);
                log.log("➡ Tentando próxima versão...");
            }
        }

        throw new RuntimeException("❌ Nenhuma versão do Firebird conseguiu conectar.");
    }

    private static Connection tentarConexao(MigracaoConfig config, LogCallback log) throws SQLException {

        String url = montarUrl(config);

        log.log("🔌 Tentando JDBC: " + url);

        try {
            DriverManager.setLoginTimeout(5);
            return DriverManager.getConnection(url, config.getFbUsuario(), config.getFbSenha());

        } catch (SQLException e) {

            // 🔥 Log mais limpo
            if (e.getMessage() != null && e.getMessage().contains("connection rejected")) {
                log.log("🚫 Porta ativa mas recusou conexão.");
            }

            throw e;
        }
    }

    private static String montarUrl(MigracaoConfig config) {

        // Exemplo:
        // jdbc:firebirdsql://localhost:3050/C:/dados/banco.fdb

        return "jdbc:firebirdsql://"
                + config.getFbHost() + ":"
                + config.getFbPorta() + "/"
                + config.getFbArquivo();
    }
}