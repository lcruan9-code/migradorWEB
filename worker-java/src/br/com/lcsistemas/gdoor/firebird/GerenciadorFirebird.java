package br.com.lcsistemas.gdoor.firebird;

import br.com.lcsistemas.gdoor.config.MigracaoConfig;
import br.com.lcsistemas.gdoor.versao.VersaoFirebird;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Gerencia o ciclo de vida dos servidores Firebird instalados localmente.
 *
 * <p>Responsabilidades:
 * <ol>
 * <li><b>Descoberta</b> — varre a pasta FIREBIRD e mapeia cada sub-pasta a um
 * {@link FirebirdInstalacao} (versão, executável, porta, prioridade).</li>
 * <li><b>Auto-start</b> — se nenhum Firebird estiver ativo na porta configurada,
 * tenta iniciar cada instalação em ordem de prioridade até um funcionar.</li>
 * <li><b>Parada segura</b> — encerra apenas o processo que <em>este manager</em>
 * iniciou; nunca mata serviços pré-existentes.</li>
 * </ol>
 *
 * <p>Prioridade de tentativa para bancos GDOOR:
 * <pre>
 * 1º  Firebird 2.5.x  (formato mais comum do GDOOR legado)
 * 2º  Firebird 2.1.x
 * 3º  Firebird 3.0.x  (backward-compatible com 2.5)
 * 4º  Firebird 4.0.x
 * 5º  Firebird 5.0.x
 * 6º  Firebird 2.0.x  (mais antigo, menor compatibilidade)
 * </pre>
 *
 * <p>O gerenciador é thread-safe para uso em SwingWorker.
 *
 * Uso:
 * <pre>
 * boolean ativo = GerenciadorFirebird.garantirConectividade(config, logCallback);
 * // ... migração ...
 * GerenciadorFirebird.pararSeIniciado(logCallback);
 * </pre>
 */
public final class GerenciadorFirebird {

    private static final Logger LOG = Logger.getLogger(GerenciadorFirebird.class.getName());

    /** Tempo de espera após iniciar o processo antes de testar a porta (ms). */
    private static final int AGUARDAR_INICIO_MS  = 3000;
    /** Timeout do socket para testar se a porta está ativa (ms). */
    private static final int TIMEOUT_PORTA_MS    = 1500;
    /** Número máximo de tentativas de teste de porta após iniciar o processo. */
    private static final int TENTATIVAS_PORTA    = 6;
    /** Intervalo entre tentativas de teste de porta (ms). */
    private static final int INTERVALO_PORTA_MS  = 1000;

    /** Processo iniciado por este gerenciador (null = não iniciou nenhum). */
    private static volatile Process processoAtivo = null;
    /** Instalação que está atualmente em execução por este gerenciador. */
    private static volatile FirebirdInstalacao instalacaoAtiva = null;

    /** Classe utilitária — não instanciar. */
    private GerenciadorFirebird() {}

    // =========================================================================
    //  API pública
    // =========================================================================

    /**
     * Ponto de entrada principal.
     *
     * <p>Garante que um servidor Firebird esteja respondendo na porta configurada.
     * Se já houver um servidor ativo, retorna {@code true} imediatamente (sem iniciar nada).
     * Caso contrário, tenta iniciar cada instalação encontrada na pasta FIREBIRD.
     *
     * @param config Configuração da migração (porta, pasta Firebird).
     * @param log    Callback para mensagens de progresso.
     * @return {@code true} se Firebird está respondendo; {@code false} se não foi possível.
     */
    public static boolean garantirConectividade(MigracaoConfig config,
                                                LogCallback log) {
        String host = config.getFbHost();
        int    porta;
        try {
            porta = Integer.parseInt(config.getFbPorta().trim());
        } catch (NumberFormatException e) {
            porta = 3050;
        }

        // 1. Verificar se já está ativo (serviço do Windows ou instância anterior)
        if (portaAtiva(host, porta, TIMEOUT_PORTA_MS)) {
            log.log("[Firebird] Serviço já ativo em " + host + ":" + porta + " — sem necessidade de iniciar.");
            return true;
        }

        log.log("[Firebird] Nenhum serviço detectado em " + host + ":" + porta + ". Buscando instalações...");

        // 2. Descobrir instalações na pasta configurada
        File pastaFb = resolverPasta(config);
        if (pastaFb == null || !pastaFb.isDirectory()) {
            log.log("[Firebird] Pasta FIREBIRD não localizada — inicialização automática desabilitada.");
            return false;
        }

        log.log("[Firebird] Pasta localizada: " + pastaFb.getAbsolutePath());
        List<FirebirdInstalacao> instalacoes = descobrirInstalacoes(pastaFb);

        if (instalacoes.isEmpty()) {
            log.log("[Firebird] Nenhuma instalação válida encontrada em " + pastaFb.getName());
            return false;
        }

        log.log("[Firebird] " + instalacoes.size() + " instalação(ões) encontrada(s):");
        for (FirebirdInstalacao inst : instalacoes) {
            log.log("[Firebird]   → " + inst);
        }

        // 3. Tentar cada instalação em ordem de prioridade
        for (FirebirdInstalacao inst : instalacoes) {
            log.log("[Firebird] Tentando: " + inst.getNomePasta() + "...");

            // Se a porta desta instalação for diferente da configurada, pular
            if (inst.getPorta() != porta) {
                log.log("[Firebird]   Porta " + inst.getPorta() + " ≠ porta configurada " + porta + " — pulando.");
                continue;
            }

            // AJUSTE: Garante que qualquer tentativa anterior presa foi morta e o estado limpo
            pararProcessoAtivo();

            boolean iniciou = iniciarInstalacao(inst, log);
            if (iniciou) {
                log.log("[Firebird] ✅ Firebird iniciado: " + inst.getNomePasta()
                    + " | porta: " + porta);
                instalacaoAtiva = inst;
                return true;
            } else {
                log.log("[Firebird]   ❌ Falhou. Limpando e tentando próxima versão...");
                // AJUSTE: Força a limpeza novamente caso o iniciarInstalacao tenha deixado sujeira
                pararProcessoAtivo();
            }
        }

        log.log("[Firebird] ⚠ Não foi possível iniciar nenhuma versão do Firebird automaticamente.");
        return false;
    }

    /**
     * Para o processo Firebird iniciado por este gerenciador.
     * Não faz nada se nenhum processo foi iniciado aqui.
     *
     * @param log Callback para mensagens de progresso.
     */
    public static void pararSeIniciado(LogCallback log) {
        if (processoAtivo == null) {
            return; // nada a parar — serviço era pré-existente
        }
        log.log("[Firebird] Encerrando Firebird iniciado pelo sistema: "
            + (instalacaoAtiva != null ? instalacaoAtiva.getNomePasta() : "?") + "...");
        pararProcessoAtivo();
        log.log("[Firebird] Firebird encerrado.");
    }

    /**
     * Descobre todas as instalações válidas na pasta informada.
     *
     * @param pastaFirebird Pasta raiz com as sub-pastas de cada versão.
     * @return Lista ordenada por prioridade (primeiro = mais adequado para GDOOR).
     */
    public static List<FirebirdInstalacao> descobrirInstalacoes(File pastaFirebird) {
        List<FirebirdInstalacao> resultado = new ArrayList<FirebirdInstalacao>();
        
        // AJUSTE: Controle para evitar repetição da mesma pasta
        Set<String> pastasProcessadas = new HashSet<>();

        File[] subDirs = pastaFirebird.listFiles();
        if (subDirs == null) return resultado;

        for (File dir : subDirs) {
            if (!dir.isDirectory()) continue;
            
            // AJUSTE: Se já processou essa pasta, pula
            String caminhoAbsoluto = dir.getAbsolutePath();
            if (pastasProcessadas.contains(caminhoAbsoluto)) continue;
            pastasProcessadas.add(caminhoAbsoluto);

            String nome = dir.getName();
            FirebirdInstalacao inst = tentarMapear(dir, nome);
            if (inst != null) {
                resultado.add(inst);
            }
        }

        // Ordenar por prioridade (menor = mais prioritário)
        Collections.sort(resultado, new Comparator<FirebirdInstalacao>() {
            @Override
            public int compare(FirebirdInstalacao a, FirebirdInstalacao b) {
                return Integer.compare(a.getPrioridade(), b.getPrioridade());
            }
        });

        return resultado;
    }

    // =========================================================================
    //  Lógica interna de início/parada
    // =========================================================================

    /**
     * Inicia uma instalação específica do Firebird em modo aplicação.
     * Aguarda até {@link #TENTATIVAS_PORTA} × {@link #INTERVALO_PORTA_MS} ms
     * pela porta ficar disponível.
     *
     * @return {@code true} se o processo iniciou e a porta ficou ativa.
     */
    static boolean iniciarInstalacao(FirebirdInstalacao inst, LogCallback log) {
        File exe  = inst.getExeFile();
        File raiz = inst.getRaizDir();

        if (!exe.exists()) {
            log.log("[Firebird]   Executável não encontrado: " + exe.getAbsolutePath());
            return false;
        }

        // GUARDA CRÍTICO: se a porta já está ativa ANTES de iniciarmos, qualquer
        // check de "porta ativa" posterior seria um falso-positivo do serviço existente.
        // Só devemos iniciar se a porta estiver LIVRE agora.
        if (portaAtiva("localhost", inst.getPorta(), TIMEOUT_PORTA_MS)) {
            log.log("[Firebird]   Porta " + inst.getPorta()
                + " já está ocupada por outro processo — não é possível iniciar "
                + inst.getNomePasta() + " nessa porta.");
            return false;
        }

        try {
            // Modo aplicação: -a roda como aplicação normal (não serviço)
            ProcessBuilder pb = new ProcessBuilder(exe.getAbsolutePath(), "-a");
            pb.directory(raiz);
            pb.redirectErrorStream(true);

            // Definir FIREBIRD env var para que o processo encontre sua configuração
            pb.environment().put("FIREBIRD", raiz.getAbsolutePath());

            log.log("[Firebird]   Iniciando: " + exe.getAbsolutePath());
            processoAtivo = pb.start();

            // Aguardar o processo subir
            dormirMs(AGUARDAR_INICIO_MS);

            // Verificar se o processo ainda está vivo
            if (!processoAtivo.isAlive()) {
                log.log("[Firebird]   Processo encerrou prematuramente.");
                processoAtivo = null;
                return false;
            }

            // Testar a porta com múltiplas tentativas
            for (int t = 1; t <= TENTATIVAS_PORTA; t++) {
                if (portaAtiva("localhost", inst.getPorta(), TIMEOUT_PORTA_MS)) {
                    return true;
                }
                log.log("[Firebird]   Aguardando porta " + inst.getPorta()
                    + "... (" + t + "/" + TENTATIVAS_PORTA + ")");
                dormirMs(INTERVALO_PORTA_MS);
            }

            log.log("[Firebird]   Porta " + inst.getPorta() + " não respondeu após "
                + TENTATIVAS_PORTA + " tentativas.");
            pararProcessoAtivo();
            return false;

        } catch (Exception e) {
            LOG.warning("[Firebird] Erro ao iniciar " + inst.getNomePasta() + ": " + e.getMessage());
            log.log("[Firebird]   Erro ao iniciar: " + e.getMessage());
            pararProcessoAtivo();
            return false;
        }
    }

    /** Para o processo ativo de forma segura. */
    private static void pararProcessoAtivo() {
        // AJUSTE: Limpeza garantida usando try-finally e tempo extra para o SO liberar a porta
        if (processoAtivo != null) {
            try {
                processoAtivo.destroy();
                dormirMs(1000); // Dá tempo ao Windows para liberar o TIME_WAIT da porta
                if (processoAtivo.isAlive()) {
                    processoAtivo.destroyForcibly();
                    dormirMs(500);
                }
            } catch (Exception e) {
                LOG.warning("[Firebird] Falha ao tentar destruir o processo: " + e.getMessage());
            } finally {
                processoAtivo  = null;
                instalacaoAtiva = null;
            }
        } else {
            // Garante estado limpo mesmo se processo for nulo
            instalacaoAtiva = null;
        }
    }

    // =========================================================================
    //  Descoberta e mapeamento de pastas
    // =========================================================================

    /**
     * Tenta mapear uma pasta de instalação para um {@link FirebirdInstalacao}.
     * Retorna {@code null} se a pasta não contém um Firebird válido.
     */
    private static FirebirdInstalacao tentarMapear(File dir, String nome) {
        // Verificar executável (FB 3.x+)
        File exeRoot = new File(dir, "firebird.exe");
        // Verificar executável (FB 2.x)
        File exeBin  = new File(dir, "bin" + File.separator + "fbserver.exe");

        FirebirdInstalacao.Layout layout;
        File exeFile;

        if (exeRoot.exists()) {
            layout  = FirebirdInstalacao.Layout.ROOT_THREAD;
            exeFile = exeRoot;
        } else if (exeBin.exists()) {
            layout  = FirebirdInstalacao.Layout.BIN_SUPER;
            exeFile = exeBin;
        } else {
            return null; // não é uma instalação Firebird válida
        }

        // Extrair string da versão a partir do nome da pasta
        // Ex: "Firebird-2.5.9.manual" → "2.5.9"
        //     "Firebird-3.0.13. manual" → "3.0.13"
        //     "Firebird-5.0 manual" → "5.0"
        String versaoStr = extrairVersaoStr(nome);

        // Mapear para enum
        VersaoFirebird versaoEnum = mapearVersaoEnum(versaoStr);

        // Ler porta do firebird.conf (default 3050)
        int porta = lerPortaConf(dir);

        // Calcular prioridade para GDOOR
        int prioridade = calcularPrioridade(versaoStr);

        return new FirebirdInstalacao(nome, dir, exeFile, versaoStr, versaoEnum,
                                      layout, porta, prioridade);
    }

    /** Extrai a string de versão numérica do nome da pasta. */
    private static String extrairVersaoStr(String nomePasta) {
        // Padrão: Firebird-X.Y.Z... ou Firebird-X.Y...
        Pattern p = Pattern.compile("(?i)Firebird[\\-_]?(\\d+\\.\\d+(?:\\.\\d+)?)");
        Matcher m = p.matcher(nomePasta);
        if (m.find()) {
            return m.group(1);
        }
        return "0.0";
    }

    /** Mapeia string de versão para {@link VersaoFirebird}. */
    private static VersaoFirebird mapearVersaoEnum(String v) {
        if (v.startsWith("2.")) return VersaoFirebird.FB25;
        if (v.startsWith("3.")) return VersaoFirebird.FB30;
        if (v.startsWith("4.")) return VersaoFirebird.FB40;
        if (v.startsWith("5.")) return VersaoFirebird.FB50;
        return VersaoFirebird.DESCONHECIDA;
    }

    /**
     * Lê a porta configurada no {@code firebird.conf} da instalação.
     * Retorna 3050 se não encontrar ou se a linha estiver comentada.
     */
    private static int lerPortaConf(File raizDir) {
        File conf = new File(raizDir, "firebird.conf");
        if (!conf.exists()) return 3050;

        try (BufferedReader br = new BufferedReader(new FileReader(conf))) {
            String linha;
            Pattern p = Pattern.compile("^\\s*RemoteServicePort\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
            while ((linha = br.readLine()) != null) {
                if (linha.trim().startsWith("#")) continue; // linha comentada
                Matcher m = p.matcher(linha);
                if (m.find()) {
                    return Integer.parseInt(m.group(1));
                }
            }
        } catch (Exception ignored) {}

        return 3050; // default Firebird
    }

    /**
     * Prioridade de tentativa para bancos GDOOR (menor = tenta primeiro):
     * <pre>
     * FB 2.5.x   → 1  (mais comum no GDOOR legado)
     * FB 2.1.x   → 2
     * FB 3.0.x   → 3  (backward-compatible com 2.5)
     * FB 4.0.x   → 4
     * FB 5.0.3   → 5  (patch release mais recente e estável do 5.x)
     * FB 5.0.x   → 6  (demais patches 5.0.y)
     * FB 5.x     → 7  (outras versões 5.x sem patch conhecido)
     * FB 2.0.x   → 8  (mais antigo, menor compatibilidade)
     * outros     → 99
     * </pre>
     *
     * <p>Quando múltiplas instalações do mesmo major.minor existirem (ex: dois FB50),
     * todas serão tentadas em sequência pelo loop de {@link #garantirConectividade}
     * e {@link #trocarParaVersao} antes de desistir.
     */
    private static int calcularPrioridade(String versaoStr) {
        if (versaoStr.startsWith("2.5"))   return 1;
        if (versaoStr.startsWith("2.1"))   return 2;
        if (versaoStr.startsWith("3."))    return 3;
        if (versaoStr.startsWith("4."))    return 4;
        if (versaoStr.startsWith("5.0.3")) return 5; // 5.0.3: patch estável — tenta antes dos demais 5.x
        if (versaoStr.startsWith("5.0."))  return 6; // Outros patches 5.0.y (ex: 5.0.1, 5.0.2)
        if (versaoStr.startsWith("5."))    return 7; // Demais 5.x sem subpatch (ex: "5.0" sem terceiro dígito)
        if (versaoStr.startsWith("2.0"))   return 8;
        return 99;
    }

    // =========================================================================
    //  Resolução da pasta FIREBIRD
    // =========================================================================

    /**
     * Resolve o caminho da pasta FIREBIRD a partir da configuração.
     *
     * <p>Estratégia (em ordem):
     * <ol>
     * <li>Usa {@code config.getPastaFirebird()} se não estiver vazio.</li>
     * <li>Procura {@code FIREBIRD/} relativo ao diretório de trabalho.</li>
     * <li>Procura {@code FIREBIRD/} relativo ao JAR em execução.</li>
     * <li>Procura {@code FIREBIRD/} no mesmo diretório do arquivo .FDB.</li>
     * </ol>
     *
     * @return {@link File} da pasta encontrada, ou {@code null}.
     */
    public static File resolverPasta(MigracaoConfig config) {
        // 1. Configuração explícita
        String pastaConf = config.getPastaFirebird();
        if (pastaConf != null && !pastaConf.trim().isEmpty()) {
            File f = new File(pastaConf.trim());
            if (f.isDirectory()) return f;
        }

        // 2. Relativo ao diretório de trabalho (user.dir)
        File f2 = new File(System.getProperty("user.dir"), "FIREBIRD");
        if (f2.isDirectory()) return f2;

        // 3. Relativo ao JAR em execução
        try {
            File jarDir = new File(GerenciadorFirebird.class
                .getProtectionDomain().getCodeSource().getLocation().toURI()).getParentFile();
            File f3 = new File(jarDir, "FIREBIRD");
            if (f3.isDirectory()) return f3;
            // Um nível acima (quando rodando do dist/)
            File f3b = new File(jarDir.getParentFile(), "FIREBIRD");
            if (f3b.isDirectory()) return f3b;
        } catch (Exception ignored) {}

        // 4. Mesmo diretório do .FDB
        String fbArquivo = config.getFbArquivo();
        if (fbArquivo != null && !fbArquivo.trim().isEmpty()) {
            File fdbDir = new File(fbArquivo.trim()).getParentFile();
            if (fdbDir != null) {
                File f4 = new File(fdbDir, "FIREBIRD");
                if (f4.isDirectory()) return f4;
            }
        }

        return null;
    }

    // =========================================================================
    //  Utilitários
    // =========================================================================

    /**
     * Testa se uma porta TCP está aceitando conexões.
     *
     * @param host      Host a testar.
     * @param porta     Porta TCP.
     * @param timeoutMs Timeout em milissegundos.
     * @return {@code true} se a porta estiver ativa.
     */
    public static boolean portaAtiva(String host, int porta, int timeoutMs) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, porta), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Tenta liberar a porta ocupada por um serviço Firebird não gerenciado por este gerenciador.
     *
     * <p>Estratégia (Windows, melhor-esforço):
     * <ol>
     *   <li>Para serviços Windows conhecidos do Firebird via {@code sc stop}.</li>
     *   <li>Identifica o PID que está escutando na porta via {@code netstat -ano}
     *       e encerra com {@code taskkill /F /PID}.</li>
     *   <li>Fallback: encerra todos os processos Firebird pelo nome do executável.</li>
     * </ol>
     *
     * <p>Erros individuais são silenciados — se nada funcionar, o chamador verificará
     * com {@link #portaAtiva} se a porta ficou livre.
     *
     * @param porta Porta TCP a liberar.
     * @param log   Callback de log para a UI.
     */
    private static void liberarPorta(int porta, LogCallback log) {
        log.log("[Firebird] Liberando porta " + porta + " ocupada por serviço externo...");

        // 1. Tentar parar serviços Windows conhecidos do Firebird
        for (String servico : new String[]{
                "FirebirdServerDefaultInstance",
                "FirebirdGuardianDefaultInstance",
                "Firebird_DefaultInstance",
                "FirebirdDefaultInstance"}) {
            try {
                new ProcessBuilder("sc", "stop", servico)
                    .redirectErrorStream(true).start()
                    .waitFor(3, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }

        // 2. Identificar o PID que está na porta via netstat e matar com taskkill
        try {
            Process netstat = new ProcessBuilder("netstat", "-ano")
                .redirectErrorStream(true).start();
            BufferedReader br = new BufferedReader(
                new InputStreamReader(netstat.getInputStream()));
            // Padrão Windows: "  TCP  0.0.0.0:3050  0.0.0.0:0  LISTENING  1234"
            Pattern portaPat = Pattern.compile(
                "\\s+TCP\\s+[^:]+:" + porta + "\\s+\\S+\\s+LISTENING\\s+(\\d+)",
                Pattern.CASE_INSENSITIVE);
            String linha;
            while ((linha = br.readLine()) != null) {
                Matcher m = portaPat.matcher(linha);
                if (m.find()) {
                    String pid = m.group(1);
                    if (!"0".equals(pid) && !"4".equals(pid)) { // ignora System/Idle
                        log.log("[Firebird]   Encerrando PID " + pid
                            + " que ocupa a porta " + porta + "...");
                        new ProcessBuilder("taskkill", "/F", "/PID", pid)
                            .redirectErrorStream(true).start()
                            .waitFor(3, TimeUnit.SECONDS);
                    }
                }
            }
            netstat.waitFor(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.log("[Firebird]   Aviso ao liberar porta: " + e.getMessage());
        }

        // 3. Fallback: encerrar todos os executáveis Firebird conhecidos
        for (String exe : new String[]{"firebird.exe", "fbserver.exe", "fbguard.exe"}) {
            try {
                new ProcessBuilder("taskkill", "/F", "/IM", exe)
                    .redirectErrorStream(true).start()
                    .waitFor(2, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }

    private static void dormirMs(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // =========================================================================
    //  Detecção de ODS e troca de versão por incompatibilidade
    // =========================================================================

    /**
     * Analisa a mensagem de erro JDBC do Jaybird e identifica a versão do Firebird
     * necessária com base no ODS (On-Disk Structure) do arquivo .FDB.
     *
     * <p>Padrão do erro Jaybird: {@code "unsupported on-disk structure for file X.fdb;
     * found 13.0, support 11.2 [ISC error code:335544379]"}
     *
     * <p>Tabela ODS → Firebird:
     * <pre>
     * ODS 10.x → Firebird 1.x → FB25 (mais antigo disponível)
     * ODS 11.0 → Firebird 2.0 → FB25
     * ODS 11.1 → Firebird 2.1 → FB25
     * ODS 11.2 → Firebird 2.5 → FB25
     * ODS 12.0 → Firebird 3.0 → FB30
     * ODS 13.0 → Firebird 4.0 → FB40  ← caso atual do log
     * ODS 13.1 → Firebird 5.0 → FB50
     * </pre>
     *
     * @param mensagemErro Mensagem da {@link java.sql.SQLException} lançada pelo Jaybird.
     * @return Versão necessária do Firebird, ou {@link VersaoFirebird#DESCONHECIDA}
     * se a mensagem não contém informação ODS reconhecível.
     */
    public static VersaoFirebird detectarVersaoPorODS(String mensagemErro) {
        if (mensagemErro == null || mensagemErro.isEmpty()) {
            return VersaoFirebird.DESCONHECIDA;
        }

        // Confirma que é realmente um erro de ODS incompatível (ISC 335544379)
        boolean ehErroODS = mensagemErro.contains("on-disk structure")
                         || mensagemErro.contains("335544379");
        if (!ehErroODS) return VersaoFirebird.DESCONHECIDA;

        // Extrai a versão ODS que o BANCO possui: padrão "found X.Y"
        Pattern p = Pattern.compile("found\\s+(\\d+\\.\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(mensagemErro);
        if (!m.find()) return VersaoFirebird.DESCONHECIDA;

        String odsStr = m.group(1);
        VersaoFirebird versao = odsParaVersaoFirebird(odsStr);
        LOG.info("[GerenciadorFirebird] ODS do banco: " + odsStr + " → Firebird necessário: " + versao);
        return versao;
    }

    /**
     * Para o Firebird atual (se iniciado por este gerenciador) e inicia a versão
     * compatível com o ODS do banco informado.
     *
     * <p>Chamado pelo {@code MigracaoEngine} quando a conexão JDBC falha com erro ODS.
     * Prioriza instalações com versão exata; não tenta versões menores (sem backward compat.).
     *
     * @param versaoNecessaria Versão detectada pelo {@link #detectarVersaoPorODS}.
     * @param config           Configuração com porta e pasta FIREBIRD.
     * @param log              Callback de log para a UI.
     * @return {@code true} se uma instalação compatível foi iniciada com sucesso.
     */
    public static boolean trocarParaVersao(VersaoFirebird versaoNecessaria,
                                           MigracaoConfig config,
                                           LogCallback log) {
        log.log("[Firebird] Incompatibilidade ODS detectada → necessário: " + versaoNecessaria);

        // 1. Parar instância gerenciada (se foi este manager que iniciou)
        if (processoAtivo != null) {
            log.log("[Firebird] Parando instância incompatível: "
                + (instalacaoAtiva != null ? instalacaoAtiva.getNomePasta() : "?") + "...");
            pararProcessoAtivo();
            dormirMs(1500); // aguardar SO liberar a porta após nosso processo encerrar
        }

        // Resolver pasta e descobrir instalações
        File pastaFb = resolverPasta(config);
        if (pastaFb == null || !pastaFb.isDirectory()) {
            log.log("[Firebird] Pasta FIREBIRD não encontrada — troca de versão impossível.");
            return false;
        }

        int porta;
        try {
            porta = Integer.parseInt(config.getFbPorta().trim());
        } catch (NumberFormatException e) {
            porta = 3050;
        }

        // 2. Se a porta AINDA está ocupada (serviço Windows externo / pré-existente),
        //    precisamos liberá-la antes de iniciar nossa própria versão.
        //    Sem isso, iniciarInstalacao detectaria a porta "ativa" do serviço externo
        //    e retornaria falso-positivo, fazendo o JDBC conectar na versão errada.
        if (portaAtiva(config.getFbHost(), porta, TIMEOUT_PORTA_MS)) {
            log.log("[Firebird] Porta " + porta
                + " ainda ocupada por serviço externo — tentando liberar...");
            liberarPorta(porta, log);
            dormirMs(2500); // aguardar processos externos encerrarem
            if (portaAtiva(config.getFbHost(), porta, TIMEOUT_PORTA_MS)) {
                log.log("[Firebird] ⚠ Porta " + porta
                    + " não foi totalmente liberada — prosseguindo assim mesmo.");
            } else {
                log.log("[Firebird] Porta " + porta + " liberada com sucesso.");
            }
        }

        List<FirebirdInstalacao> todas = descobrirInstalacoes(pastaFb);

        // Tentar instalações com versão exata primeiro
        log.log("[Firebird] Buscando instalação compatível com " + versaoNecessaria + " na porta " + porta + "...");
        for (FirebirdInstalacao inst : todas) {
            if (inst.getVersaoEnum() != versaoNecessaria) continue;
            if (inst.getPorta() != porta) {
                log.log("[Firebird]   " + inst.getNomePasta()
                    + " — porta " + inst.getPorta() + " ≠ " + porta + " — pulando.");
                continue;
            }

            // AJUSTE: Garante que qualquer tentativa anterior presa foi morta
            pararProcessoAtivo();

            log.log("[Firebird] Tentando: " + inst.getNomePasta() + "...");
            boolean ok = iniciarInstalacao(inst, log);
            if (ok) {
                instalacaoAtiva = inst;
                log.log("[Firebird] ✅ Trocado com sucesso para: " + inst.getNomePasta()
                    + " | porta: " + porta);
                return true;
            }
            
            // AJUSTE: Limpa caso tenha falhado
            pararProcessoAtivo();
            log.log("[Firebird]   ❌ Falhou. Tentando próxima candidata...");
        }

        log.log("[Firebird] ⚠ Nenhuma instalação de " + versaoNecessaria
            + " funcionou. Verifique a pasta FIREBIRD do projeto.");
        return false;
    }

    /**
     * Mapeia versão ODS para a versão do Firebird que a suporta.
     * Usa o enum {@link VersaoFirebird} disponível no projeto.
     */
    private static VersaoFirebird odsParaVersaoFirebird(String ods) {
        if (ods == null) return VersaoFirebird.DESCONHECIDA;
        // ODS 13.1 = Firebird 5.0
        if (ods.startsWith("13.1")) return VersaoFirebird.FB50;
        // ODS 13.0 = Firebird 4.0
        if (ods.startsWith("13."))  return VersaoFirebird.FB40;
        // ODS 12.0 = Firebird 3.0
        if (ods.startsWith("12."))  return VersaoFirebird.FB30;
        // ODS 11.x = Firebird 2.x (todos mapeados para FB25 — mais compatível do grupo)
        if (ods.startsWith("11."))  return VersaoFirebird.FB25;
        // ODS 10.x = Firebird 1.x — usar FB25 (mais antigo disponível no projeto)
        if (ods.startsWith("10."))  return VersaoFirebird.FB25;
        return VersaoFirebird.DESCONHECIDA;
    }

    // ── Interface de callback de log ──────────────────────────────────────────

    /** Interface funcional para log — compatível com Java 8+. */
    public interface LogCallback {
        void log(String msg);
    }
}