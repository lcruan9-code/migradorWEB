package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Statement;

/**
 * Step responsável por aplicar ajustes no banco MySQL (lc_sistemas) após
 * a conclusão da migração dos dados do Firebird.
 *
 * Lê os scripts contidos na pasta BASE local e os executa.
 * Efetua a limpeza de strings (acentos, espaços) e executa as procedures 
 * relacionadas a telefone e tributação.
 */
public class AjustePosMigracaoStep extends StepBase {

    @Override
    public String getNome() {
        return "AjustePosMigracaoStep";
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        LOG.info("[AjustePosMigracaoStep] Iniciando rotinas de pós-migração...");
        
        // Caminhos dos scripts na pasta BASE
        File dirBase = new File("BASE");
        if (!dirBase.exists() || !dirBase.isDirectory()) {
            LOG.warning("[AjustePosMigracaoStep] Pasta BASE/ não encontrada. Os scripts de ajuste não serão executados.");
            return;
        }

        File procTelefone   = new File(dirBase, "procedure do telefone.txt");
        File procTributacao = new File(dirBase, "procedure de tributacao.txt");
        File scriptAjuste   = new File(dirBase, "script de ajuste completo.txt");

        // 1. Criar as procedures necessárias no MySQL
        executarScriptProcedure(ctx, procTelefone, "Procedure de Telefone");
        executarScriptProcedure(ctx, procTributacao, "Procedure NOVOGRUPOTRIB2");

        // 2. Executar script de ajuste (ininterruptamente via multi-querying JDBC)
        executarScriptIntegral(ctx, scriptAjuste, "Ajuste Completo");

        LOG.info("[AjustePosMigracaoStep] Rotinas de pós-migração concluídas.");
    }

    /**
     * Lê um arquivo de script no formato DDL (Procedure) e executa na conexão destino.
     */
    private void executarScriptProcedure(MigracaoContext ctx, File arquivo, String descricao) {
        if (!arquivo.exists()) {
            LOG.warning("[AjustePosMigracaoStep] Arquivo não encontrado: " + arquivo.getName());
            return;
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(arquivo))) {
            String linha;
            while ((linha = br.readLine()) != null) {
                sb.append(linha).append("\n");
            }
            LOG.info("[AjustePosMigracaoStep] Criando " + descricao + "...");
            execIgnore(ctx.getDestinoConn(), sb.toString(), descricao);
        } catch (Exception e) {
            LOG.warning("[AjustePosMigracaoStep] Erro ao ler/executar " + arquivo.getName() + ": " + e.getMessage());
        }
    }



    /**
     * Usa a flag allowMultiQueries=true da conexão para passar o arquivo inteiro nativamente para o MySQL,
     * mitigando gargalos de parser para inserts/comandos monstruosamente compridos em arquivos DBeaver de ANSI.
     */
    private void executarScriptIntegral(MigracaoContext ctx, File arquivo, String descricao) {
        if (!arquivo.exists()) {
            LOG.warning("[AjustePosMigracaoStep] Arquivo não encontrado: " + arquivo.getName());
            return;
        }
        LOG.info("[AjustePosMigracaoStep] Executando bloco integral " + descricao + " (" + arquivo.getName() + ")...");
        try {
            byte[] encoded = java.nio.file.Files.readAllBytes(arquivo.toPath());
            String sqlTotal = new String(encoded, java.nio.charset.StandardCharsets.ISO_8859_1);
            
            try (Statement st = ctx.getDestinoConn().createStatement()) {
                st.execute(sqlTotal);
                LOG.info("[AjustePosMigracaoStep] Script Integral " + descricao + " finalizado com sucesso!");
            }
        } catch (Exception e) {
            LOG.warning("[AjustePosMigracaoStep] Erro fatal na execucao integral de " + arquivo.getName() + ": " + e.getMessage());
        }
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        // Nada específico para desfazer, os inserts originais já serão perdidos no truncamento das tabelas
    }
}
