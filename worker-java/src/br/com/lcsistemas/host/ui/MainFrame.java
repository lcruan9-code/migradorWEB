package br.com.lcsistemas.host.ui;

import br.com.lcsistemas.host.config.MigracaoConfig;
import br.com.lcsistemas.host.engine.MigracaoEngine;
import com.formdev.flatlaf.FlatClientProperties;
import com.formdev.flatlaf.FlatLightLaf;

import java.awt.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.plaf.basic.BasicTabbedPaneUI;

/**
 * Tela principal do HOST Migration Engine.
 *
 * Navegação:  sidebar lateral com botões que trocam a aba visível
 *             (JTabbedPane com barra de abas escondida — idêntico ao gdoor-migration).
 *
 * Aba 1 — Destino MySQL  (lc_sistemas)
 * Aba 2 — Origem MySQL   (schema "host") ← substitui Firebird
 * Aba 3 — Cliente / Empresa
 * Aba 4 — Migração (log, progresso)
 */
public class MainFrame extends JFrame implements MigracaoEngine.ProgressListener {

    private static final Logger LOG = Logger.getLogger(MainFrame.class.getName());

    // ── Conexão MySQL mantida para carregar combos ───────────────────────────
    private Connection mysqlConn;

    // ── Engine ───────────────────────────────────────────────────────────────
    private MigracaoEngine engine;

    // ── Botão de navegação activo ─────────────────────────────────────────────
    private JButton btnNavAtivo;

    // =========================================================================
    //  Componentes declarados globalmente (igual ao gdoor — acesso por handlers)
    // =========================================================================

    // ── Sidebar ───────────────────────────────────────────────────────────────
    private JPanel     pnlMain;
    private JPanel     pnlSidebar;
    private JLabel     lblSubtitulo;
    private JSeparator jSeparator1;
    private JLabel     lblSecaoConfig;
    private JButton    btnNavMySQL;
    private JButton    btnNavOrigem;   // era btnNavFirebird — agora "Origem HOST"
    private JButton    btnNavCliente;
    private JSeparator jSeparator2;
    private JLabel     lblSecaoExec;
    private JButton    btnNavMigracao;
    private JLabel     lblVersao1;
    private JLabel     jlogo;

    // ── TabbedPane ────────────────────────────────────────────────────────────
    private JTabbedPane tabPrincipal;
    private JPanel      tabDestino;
    private JPanel      tabOrigem;
    private JPanel      tabCliente;
    private JPanel      tabMigracao;

    // ── Destino MySQL ─────────────────────────────────────────────────────────
    private JTextField    txtMyHost;
    private JTextField    txtMyPorta;
    private JTextField    txtMyDatabase;
    private JTextField    txtMyUsuario;
    private JPasswordField txtMySenha;
    private JButton       btnTestarMySQL;

    // ── Origem Firebird ───────────────────────────────────────────────────────
    private JTextField     txtOrigemHost;
    private JTextField     txtOrigemPorta;
    private JTextField     txtOrigemArquivoFdb;
    private JButton        btnBrowseFdb;
    private JTextField     txtOrigemUsuario;
    private JPasswordField txtOrigemSenha;
    private JButton        btnTestarOrigem;

    // ── Cliente ───────────────────────────────────────────────────────────────
    private JTextField    txtClienteCnpj;
    private JComboBox     cmbUf;
    private JComboBox     cmbCidade;
    private JComboBox     cmbRegimeTributario;
    private JTextField    txtEmpresaId;
    private JButton       btnCarregarCidades;

    // ── Migração ──────────────────────────────────────────────────────────────
    private JLabel        lblStep;
    private JProgressBar  prgSteps;
    private JScrollPane   scrLog;
    private JTextArea     txtLog;
    private JButton       btnIniciar;
    private JButton       btnCancelar;

    // ── Status bar ─────────────────────────────────────────────────────────────
    private JPanel  pnlStatusBar;
    private JLabel  lblStatus;

    // =========================================================================
    //  Construtor
    // =========================================================================

    public MainFrame() {
        super();
        initComponents();
        botaoPrimario(btnTestarMySQL);
        botaoPrimario(btnTestarOrigem);
        botaoPrimario(btnIniciar);
        botaoPrimario(btnCarregarCidades);
        aplicarEstilosCores();
        setLocationRelativeTo(null);
        setTitle("LC Sistemas — HOST Migration Engine v1.0");
    }

    // =========================================================================
    //  initComponents  (estilo NetBeans GUI Builder — totalmente programático)
    // =========================================================================

    private void initComponents() {

        // ── Componentes ───────────────────────────────────────────────────────
        pnlMain    = new JPanel();
        pnlSidebar = new JPanel();

        lblSubtitulo  = new JLabel("Migração HOST");
        jSeparator1   = new JSeparator();
        lblSecaoConfig = new JLabel("CONFIGURAÇÃO");
        btnNavMySQL   = new JButton("  Destino MySQL");
        btnNavOrigem  = new JButton("  Origem HOST");
        btnNavCliente  = new JButton("  Cliente");
        jSeparator2   = new JSeparator();
        lblSecaoExec  = new JLabel("EXECUÇÃO");
        btnNavMigracao = new JButton("  Migração");
        lblVersao1    = new JLabel("v1.0");
        jlogo         = new JLabel();

        tabPrincipal  = new JTabbedPane();
        tabDestino    = new JPanel();
        tabOrigem     = new JPanel();
        tabCliente    = new JPanel();
        tabMigracao   = new JPanel();

        // Destino
        txtMyHost     = new JTextField("localhost");
        txtMyPorta    = new JTextField("3306");
        txtMyDatabase = new JTextField("lc_sistemas");
        txtMyUsuario  = new JTextField("root");
        txtMySenha    = new JPasswordField("123456");
        btnTestarMySQL = new JButton("Conexão MySQL");

        // Origem (Firebird)
        txtOrigemHost        = new JTextField("localhost");
        txtOrigemPorta       = new JTextField("3050");
        txtOrigemArquivoFdb  = new JTextField("");
        btnBrowseFdb         = new JButton("...");
        txtOrigemUsuario     = new JTextField("SYSDBA");
        txtOrigemSenha       = new JPasswordField("masterkey");
        btnTestarOrigem      = new JButton("Conexão Firebird");

        // Cliente
        txtClienteCnpj      = new JTextField("");
        cmbUf               = new JComboBox();
        cmbCidade           = new JComboBox();
        cmbRegimeTributario = new JComboBox(new String[]{"Simples Nacional", "Regime Normal"});
        txtEmpresaId        = new JTextField("1");
        btnCarregarCidades  = new JButton("↺ Recarregar");

        // Migração
        lblStep    = new JLabel("Aguardando início...");
        prgSteps   = new JProgressBar();
        txtLog     = new JTextArea();
        scrLog     = new JScrollPane();
        btnIniciar  = new JButton("▶  Iniciar");
        btnCancelar = new JButton("■  Cancelar");

        // Status bar
        pnlStatusBar = new JPanel();
        lblStatus    = new JLabel("Pronto. Conecte ao MySQL (Aba 1) para carregar UF e Cidade.");

        // ── Configurações de janela ───────────────────────────────────────────
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setTitle("LC Sistemas — HOST Migration Engine v1.0");
        setMinimumSize(new Dimension(950, 620));

        // ── pnlMain ──────────────────────────────────────────────────────────
        pnlMain.setBackground(new Color(37, 37, 37));
        pnlMain.setLayout(new BorderLayout());

        // ── Sidebar ───────────────────────────────────────────────────────────
        pnlSidebar.setBackground(new Color(63, 42, 115));
        pnlSidebar.setForeground(new Color(63, 42, 115));
        pnlSidebar.setPreferredSize(new Dimension(195, 0));

        lblSubtitulo.setBackground(new Color(63, 42, 115));
        lblSubtitulo.setFont(new Font("SansSerif", Font.PLAIN, 11));
        lblSubtitulo.setForeground(new Color(130, 130, 130));
        lblSubtitulo.setBorder(BorderFactory.createEmptyBorder(0, 16, 0, 10));

        jSeparator1.setBackground(new Color(63, 42, 115));
        jSeparator1.setForeground(new Color(63, 42, 115));

        lblSecaoConfig.setFont(new Font("SansSerif", Font.PLAIN, 10));
        lblSecaoConfig.setForeground(Color.WHITE);
        lblSecaoConfig.setBorder(BorderFactory.createEmptyBorder(14, 16, 6, 10));

        // Botões de navegação da sidebar
        estilizarBtnNav(btnNavMySQL);
        estilizarBtnNav(btnNavOrigem);
        estilizarBtnNav(btnNavCliente);

        jSeparator2.setBackground(new Color(63, 42, 115));
        jSeparator2.setForeground(new Color(63, 42, 115));

        lblSecaoExec.setFont(new Font("SansSerif", Font.PLAIN, 10));
        lblSecaoExec.setForeground(Color.WHITE);
        lblSecaoExec.setBorder(BorderFactory.createEmptyBorder(14, 16, 6, 10));

        estilizarBtnNav(btnNavMigracao);

        lblVersao1.setFont(new Font("SansSerif", Font.PLAIN, 10));
        lblVersao1.setForeground(new Color(101, 101, 101));
        lblVersao1.setBorder(BorderFactory.createEmptyBorder(6, 16, 0, 10));

        // Logo (usa o mesmo ícone do gdoor se existir, senão texto)
        try {
            java.net.URL url = getClass().getResource("/br/com/lcsistemas/gdoor/icon/lc_logoSofthouse_para_fundo_escuro2.png");
            if (url != null) {
                ImageIcon icon = new ImageIcon(url);
                Image scaled = icon.getImage().getScaledInstance(160, 60, Image.SCALE_SMOOTH);
                jlogo.setIcon(new ImageIcon(scaled));
            } else {
                jlogo.setText("LC Sistemas");
                jlogo.setForeground(Color.WHITE);
                jlogo.setFont(new Font("SansSerif", Font.BOLD, 12));
            }
        } catch (Exception e) {
            jlogo.setText("LC Sistemas");
            jlogo.setForeground(Color.WHITE);
        }
        jlogo.setHorizontalAlignment(SwingConstants.CENTER);

        // Layout da sidebar (GroupLayout para fidelidade ao gdoor)
        GroupLayout sidebarLayout = new GroupLayout(pnlSidebar);
        pnlSidebar.setLayout(sidebarLayout);
        sidebarLayout.setHorizontalGroup(
            sidebarLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addComponent(jSeparator1)
            .addComponent(lblSecaoConfig, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jSeparator2)
            .addComponent(lblSecaoExec, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addGroup(GroupLayout.Alignment.TRAILING, sidebarLayout.createSequentialGroup()
                .addGap(0, 28, Short.MAX_VALUE)
                .addGroup(sidebarLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
                    .addGroup(GroupLayout.Alignment.TRAILING, sidebarLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                        .addComponent(btnNavCliente,  GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnNavOrigem,   GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnNavMySQL,    GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 167, Short.MAX_VALUE))
                    .addComponent(btnNavMigracao, GroupLayout.Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 160, GroupLayout.PREFERRED_SIZE)))
            .addGroup(sidebarLayout.createSequentialGroup()
                .addGroup(sidebarLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
                    .addGroup(GroupLayout.Alignment.TRAILING, sidebarLayout.createSequentialGroup()
                        .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addGroup(sidebarLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
                            .addComponent(lblVersao1, GroupLayout.Alignment.TRAILING)
                            .addComponent(lblSubtitulo, GroupLayout.Alignment.TRAILING)))
                    .addComponent(jlogo, GroupLayout.PREFERRED_SIZE, 0, Short.MAX_VALUE))
                .addContainerGap())
        );
        sidebarLayout.setVerticalGroup(
            sidebarLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(sidebarLayout.createSequentialGroup()
                .addGap(84, 84, 84)
                .addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(lblSecaoConfig)
                .addGap(0, 0, 0)
                .addComponent(btnNavMySQL,    GroupLayout.PREFERRED_SIZE, 42, GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(btnNavOrigem,   GroupLayout.PREFERRED_SIZE, 42, GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(btnNavCliente,  GroupLayout.PREFERRED_SIZE, 42, GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(jSeparator2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(lblSecaoExec)
                .addGap(0, 0, 0)
                .addComponent(btnNavMigracao, GroupLayout.PREFERRED_SIZE, 42, GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, 137, Short.MAX_VALUE)
                .addComponent(jlogo, GroupLayout.PREFERRED_SIZE, 60, GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(lblSubtitulo)
                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(lblVersao1)
                .addContainerGap())
        );

        pnlMain.add(pnlSidebar, BorderLayout.WEST);

        // ── TabbedPane ────────────────────────────────────────────────────────
        tabPrincipal.setFont(new Font("SansSerif", Font.PLAIN, 12));

        buildTabDestino();
        buildTabOrigem();
        buildTabCliente();
        buildTabMigracao();

        tabPrincipal.addTab("1 - Destino (MySQL)",   tabDestino);
        tabPrincipal.addTab("2 - Origem (Firebird)",  tabOrigem);
        tabPrincipal.addTab("3 - Cliente",            tabCliente);
        tabPrincipal.addTab("4 - Migração",          tabMigracao);

        pnlMain.add(tabPrincipal, BorderLayout.CENTER);

        // ── Status bar ────────────────────────────────────────────────────────
        pnlStatusBar.setBackground(Color.WHITE);
        pnlStatusBar.setBorder(BorderFactory.createEtchedBorder());

        lblStatus.setBackground(new Color(51, 51, 51));

        GroupLayout statusLayout = new GroupLayout(pnlStatusBar);
        pnlStatusBar.setLayout(statusLayout);
        statusLayout.setHorizontalGroup(
            statusLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(statusLayout.createSequentialGroup()
                .addGap(5, 5, 5)
                .addComponent(lblStatus, GroupLayout.PREFERRED_SIZE, 725, GroupLayout.PREFERRED_SIZE)
                .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        statusLayout.setVerticalGroup(
            statusLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(statusLayout.createSequentialGroup()
                .addGap(3, 3, 3)
                .addComponent(lblStatus)
                .addGap(3, 3, 3))
        );

        pnlMain.add(pnlStatusBar, BorderLayout.SOUTH);

        // ── Montar o contentPane ──────────────────────────────────────────────
        GroupLayout mainLayout = new GroupLayout(getContentPane());
        getContentPane().setLayout(mainLayout);
        mainLayout.setHorizontalGroup(
            mainLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addComponent(pnlMain, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        mainLayout.setVerticalGroup(
            mainLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addComponent(pnlMain, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );

        pack();
        setLocationRelativeTo(null);

        // ── Ações dos botões de navegação ─────────────────────────────────────
        btnNavMySQL.addActionListener(e    -> { tabPrincipal.setSelectedComponent(tabDestino);  marcarNavAtivo(btnNavMySQL);  });
        btnNavOrigem.addActionListener(e   -> { tabPrincipal.setSelectedComponent(tabOrigem);   marcarNavAtivo(btnNavOrigem); });
        btnNavCliente.addActionListener(e  -> { tabPrincipal.setSelectedComponent(tabCliente);  marcarNavAtivo(btnNavCliente); });
        btnNavMigracao.addActionListener(e -> { tabPrincipal.setSelectedComponent(tabMigracao); marcarNavAtivo(btnNavMigracao); });
    }

    // =========================================================================
    //  Construção das abas
    // =========================================================================

    private void buildTabDestino() {
        JLabel lblMyHost     = new JLabel("Host:");
        JLabel lblMyPorta    = new JLabel("Porta:");
        JLabel lblMyDatabase = new JLabel("Banco (destino):");
        JLabel lblMyUsuario  = new JLabel("Usuário:");
        JLabel lblMySenha    = new JLabel("Senha:");

        txtMyHost.setPreferredSize(null);
        txtMyPorta.setPreferredSize(null);
        txtMyDatabase.setPreferredSize(null);
        txtMyUsuario.setPreferredSize(null);
        txtMySenha.setPreferredSize(null);

        btnTestarMySQL.addActionListener(e -> btnTestarMySQLAction());

        GroupLayout gl = new GroupLayout(tabDestino);
        tabDestino.setLayout(gl);
        gl.setHorizontalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(gl.createSequentialGroup()
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.LEADING)
                    .addGroup(gl.createSequentialGroup()
                        .addGap(30, 30, 30)
                        .addGroup(gl.createParallelGroup(GroupLayout.Alignment.TRAILING)
                            .addComponent(lblMyHost).addComponent(lblMyPorta)
                            .addComponent(lblMyDatabase).addComponent(lblMyUsuario).addComponent(lblMySenha))
                        .addGap(10, 10, 10)
                        .addGroup(gl.createParallelGroup(GroupLayout.Alignment.LEADING)
                            .addComponent(txtMyPorta, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMyHost, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMySenha, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMyUsuario, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMyDatabase, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 407, Short.MAX_VALUE)))
                    .addGroup(GroupLayout.Alignment.TRAILING, gl.createSequentialGroup()
                        .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnTestarMySQL, GroupLayout.PREFERRED_SIZE, 140, GroupLayout.PREFERRED_SIZE)))
                .addGap(12, 12, 12))
        );
        gl.setVerticalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(gl.createSequentialGroup()
                .addGap(30, 30, 30)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyHost).addComponent(txtMyHost, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyPorta).addComponent(txtMyPorta, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyDatabase).addComponent(txtMyDatabase, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyUsuario).addComponent(txtMyUsuario, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMySenha).addComponent(txtMySenha, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, 300, Short.MAX_VALUE)
                .addComponent(btnTestarMySQL, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                .addGap(12, 12, 12))
        );
    }

    private void buildTabOrigem() {
        JLabel lblInfo  = new JLabel(
            "<html><b>Banco de origem:</b> Firebird (.fdb)<br>" +
            "Porta padrão: <b>3050</b> &nbsp;&nbsp; Usuário padrão: <b>SYSDBA</b><br>" +
            "Selecione o arquivo <code>.fdb</code> com o botão <b>...</b>" +
            "</html>");
        lblInfo.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createLineBorder(new Color(63, 42, 115), 1, true),
            BorderFactory.createEmptyBorder(8, 12, 8, 12)));
        lblInfo.setFont(new Font("SansSerif", Font.PLAIN, 11));

        JLabel lblOrigemHost     = new JLabel("Host:");
        JLabel lblOrigemPorta    = new JLabel("Porta:");
        JLabel lblOrigemFdb      = new JLabel("Arquivo .fdb:");
        JLabel lblOrigemUsuario  = new JLabel("Usuário:");
        JLabel lblOrigemSenha    = new JLabel("Senha:");

        // Botão Browse para selecionar o .fdb
        btnBrowseFdb.setToolTipText("Selecionar arquivo Firebird (.fdb)");
        btnBrowseFdb.addActionListener(e -> {
            JFileChooser fc = new JFileChooser();
            fc.setDialogTitle("Selecionar banco Firebird");
            fc.setFileFilter(new FileNameExtensionFilter("Banco Firebird (*.fdb, *.gdb)", "fdb", "gdb"));
            fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
            if (fc.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
                txtOrigemArquivoFdb.setText(fc.getSelectedFile().getAbsolutePath());
            }
        });

        btnTestarOrigem.addActionListener(e -> btnTestarOrigemAction());

        // Painel para arquivo .fdb + botão
        JPanel pnlFdb = new JPanel(new BorderLayout(4, 0));
        pnlFdb.setOpaque(false);
        pnlFdb.add(txtOrigemArquivoFdb, BorderLayout.CENTER);
        pnlFdb.add(btnBrowseFdb, BorderLayout.EAST);

        GroupLayout gl = new GroupLayout(tabOrigem);
        tabOrigem.setLayout(gl);
        gl.setHorizontalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(gl.createSequentialGroup()
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.LEADING)
                    .addGroup(gl.createSequentialGroup()
                        .addGap(30, 30, 30)
                        .addGroup(gl.createParallelGroup(GroupLayout.Alignment.TRAILING)
                            .addComponent(lblOrigemHost).addComponent(lblOrigemPorta)
                            .addComponent(lblOrigemFdb).addComponent(lblOrigemUsuario).addComponent(lblOrigemSenha))
                        .addGap(10, 10, 10)
                        .addGroup(gl.createParallelGroup(GroupLayout.Alignment.LEADING)
                            .addComponent(txtOrigemHost, GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)
                            .addComponent(txtOrigemPorta, GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)
                            .addComponent(pnlFdb, GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)
                            .addComponent(txtOrigemUsuario, GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)
                            .addComponent(txtOrigemSenha, GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)))
                    .addGroup(gl.createSequentialGroup()
                        .addGap(30).addComponent(lblInfo, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                    .addGroup(GroupLayout.Alignment.TRAILING, gl.createSequentialGroup()
                        .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnTestarOrigem, GroupLayout.PREFERRED_SIZE, 150, GroupLayout.PREFERRED_SIZE)))
                .addGap(12, 12, 12))
        );
        gl.setVerticalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(gl.createSequentialGroup()
                .addGap(20)
                .addComponent(lblInfo)
                .addGap(20)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblOrigemHost).addComponent(txtOrigemHost, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblOrigemPorta).addComponent(txtOrigemPorta, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.CENTER)
                    .addComponent(lblOrigemFdb).addComponent(pnlFdb, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblOrigemUsuario).addComponent(txtOrigemUsuario, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addGap(8).addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblOrigemSenha).addComponent(txtOrigemSenha, GroupLayout.PREFERRED_SIZE, 25, GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, 260, Short.MAX_VALUE)
                .addComponent(btnTestarOrigem, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                .addGap(12))
        );
    }

    private void buildTabCliente() {
        JLabel lblClienteNome    = new JLabel("CNPJ:");
        JLabel lblClienteUf      = new JLabel("UF:");
        JLabel lblClienteCidade  = new JLabel("Cidade Padrão:");
        JLabel lblRegime         = new JLabel("Regime Tributário:");
        JLabel lblEmpresaId      = new JLabel("ID Empresa:");

        cmbUf.addActionListener(e -> {
            EstadoItem est = (EstadoItem) cmbUf.getSelectedItem();
            if (est != null) carregarCidades(est.iduf);
        });
        btnCarregarCidades.addActionListener(e -> carregarEstados());

        GroupLayout gl = new GroupLayout(tabCliente);
        tabCliente.setLayout(gl);
        gl.setHorizontalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(gl.createSequentialGroup()
                .addGap(30, 30, 30)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.TRAILING)
                    .addComponent(lblClienteNome).addComponent(lblClienteUf)
                    .addComponent(lblClienteCidade).addComponent(lblRegime).addComponent(lblEmpresaId))
                .addGap(10, 10, 10)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.LEADING)
                    .addComponent(txtClienteCnpj, GroupLayout.DEFAULT_SIZE, 395, Short.MAX_VALUE)
                    .addComponent(cmbUf, 0, 395, Short.MAX_VALUE)
                    .addComponent(cmbCidade, 0, 395, Short.MAX_VALUE)
                    .addGroup(gl.createSequentialGroup()
                        .addComponent(cmbRegimeTributario, GroupLayout.PREFERRED_SIZE, 200, GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 0, Short.MAX_VALUE))
                    .addComponent(txtEmpresaId, GroupLayout.DEFAULT_SIZE, 395, Short.MAX_VALUE)
                    .addGroup(GroupLayout.Alignment.TRAILING, gl.createSequentialGroup()
                        .addGap(0, 0, Short.MAX_VALUE)
                        .addComponent(btnCarregarCidades, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)))
                .addGap(12, 12, 12))
        );
        gl.setVerticalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(gl.createSequentialGroup()
                .addGap(30, 30, 30)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblClienteNome).addComponent(txtClienteCnpj, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGap(12)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblClienteUf).addComponent(cmbUf, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGap(13)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblClienteCidade).addComponent(cmbCidade, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGap(12)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblRegime).addComponent(cmbRegimeTributario, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addGap(12)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(lblEmpresaId).addComponent(txtEmpresaId, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, 298, Short.MAX_VALUE)
                .addComponent(btnCarregarCidades, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                .addGap(12))
        );
    }

    private void buildTabMigracao() {
        lblStep.setFont(new Font("SansSerif", Font.BOLD, 12));

        prgSteps.setStringPainted(true);

        txtLog.setEditable(false);
        txtLog.setColumns(20);
        txtLog.setFont(new Font("Monospaced", Font.PLAIN, 12));
        txtLog.setRows(10);
        scrLog.setViewportView(txtLog);

        btnIniciar.setFont(new Font("SansSerif", Font.BOLD, 13));
        btnIniciar.addActionListener(e -> btnIniciarAction());

        btnCancelar.setText("■  Cancelar");
        btnCancelar.setEnabled(false);
        btnCancelar.addActionListener(e -> btnCancelarAction());

        GroupLayout gl = new GroupLayout(tabMigracao);
        tabMigracao.setLayout(gl);
        gl.setHorizontalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(GroupLayout.Alignment.TRAILING, gl.createSequentialGroup()
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.TRAILING)
                    .addGroup(gl.createSequentialGroup()
                        .addContainerGap(309, Short.MAX_VALUE)
                        .addComponent(btnCancelar)
                        .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(btnIniciar, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE))
                    .addGroup(gl.createSequentialGroup()
                        .addGap(8, 8, 8)
                        .addGroup(gl.createParallelGroup(GroupLayout.Alignment.LEADING)
                            .addComponent(prgSteps, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addGroup(gl.createSequentialGroup()
                                .addComponent(lblStep)
                                .addGap(0, 0, Short.MAX_VALUE))
                            .addComponent(scrLog))))
                .addGap(12, 12, 12))
        );
        gl.setVerticalGroup(
            gl.createParallelGroup(GroupLayout.Alignment.LEADING)
            .addGroup(gl.createSequentialGroup()
                .addGap(10, 10, 10)
                .addComponent(lblStep)
                .addGap(4, 4, 4)
                .addComponent(prgSteps, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addGap(8, 8, 8)
                .addComponent(scrLog, GroupLayout.DEFAULT_SIZE, 423, Short.MAX_VALUE)
                .addGap(12, 12, 12)
                .addGroup(gl.createParallelGroup(GroupLayout.Alignment.BASELINE)
                    .addComponent(btnIniciar, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnCancelar, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE))
                .addGap(12, 12, 12))
        );
    }

    // =========================================================================
    //  Handlers de eventos
    // =========================================================================

    private void btnTestarMySQLAction() {
        String url  = buildUrlMySQL();
        String user = txtMyUsuario.getText().trim();
        String pass = new String(txtMySenha.getPassword());
        setStatus("Conectando ao MySQL (destino)...");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            try { Class.forName("com.mysql.jdbc.Driver"); } catch (ClassNotFoundException ignored) {}
        }
        try {
            if (mysqlConn != null && !mysqlConn.isClosed()) mysqlConn.close();
            mysqlConn = DriverManager.getConnection(url, user, pass);
            JOptionPane.showMessageDialog(this,
                "Conexão MySQL OK!\n",
                "Conexão OK", JOptionPane.INFORMATION_MESSAGE);
            setStatus("MySQL OK. Carregando UFs e Cidades...");
            carregarEstados();
            // Avança para aba Origem (igual ao gdoor que avança para aba Firebird)
            tabPrincipal.setSelectedComponent(tabOrigem);
            marcarNavAtivo(btnNavOrigem);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this,
                "Falha ao conectar MySQL:\n" + ex.getMessage(),
                "Erro de Conexão", JOptionPane.ERROR_MESSAGE);
            setStatus("Erro MySQL: " + ex.getMessage());
        }
    }

    private void btnTestarOrigemAction() {
        String fdb  = txtOrigemArquivoFdb.getText().trim();
        if (fdb.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                "Selecione o arquivo .fdb do banco Firebird.",
                "Arquivo não informado", JOptionPane.WARNING_MESSAGE);
            return;
        }
        String url  = buildUrlOrigem();
        String user = txtOrigemUsuario.getText().trim();
        String pass = new String(txtOrigemSenha.getPassword());
        setStatus("Conectando ao Firebird (origem)...");
        try {
            Class.forName("org.firebirdsql.jdbc.FBDriver");
        } catch (ClassNotFoundException e) {
            JOptionPane.showMessageDialog(this,
                "Driver Jaybird não encontrado no classpath.\n" +
                "Adicione jaybird-full-X.X.X.jar às bibliotecas do projeto.",
                "Driver ausente", JOptionPane.ERROR_MESSAGE);
            setStatus("Driver Jaybird não encontrado.");
            return;
        }
        try (Connection c = DriverManager.getConnection(url, user, pass);
             Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM RDB$RELATIONS");
            rs.next();
            int tabelas = rs.getInt(1);
            JOptionPane.showMessageDialog(this,
                "Conexão com o Firebird OK!\n\nTabelas encontradas: " + tabelas
                + "\n\nAvançando para configuração do cliente...",
                "Conexão OK", JOptionPane.INFORMATION_MESSAGE);
            setStatus("Firebird OK — " + tabelas + " tabelas. Preencha os dados do cliente.");
            tabPrincipal.setSelectedComponent(tabCliente);
            marcarNavAtivo(btnNavCliente);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this,
                "Falha ao conectar Firebird:\n" + ex.getMessage(),
                "Erro de Conexão", JOptionPane.ERROR_MESSAGE);
            setStatus("Erro Firebird: " + ex.getMessage());
        }
    }

    private void btnIniciarAction() {
        MigracaoConfig config = montarConfig();
        if (config == null) return;

        btnIniciar.setEnabled(false);
        btnCancelar.setEnabled(true);
        txtLog.setText("");
        prgSteps.setValue(0);
        prgSteps.setMaximum(12);
        prgSteps.setString("0 / 12");
        tabPrincipal.setSelectedComponent(tabMigracao);
        marcarNavAtivo(btnNavMigracao);

        engine = new MigracaoEngine(config);
        engine.setListener(this);
        engine.executar();
    }

    private void btnCancelarAction() {
        if (engine != null) {
            engine.cancelar();
            setStatus("Cancelando...");
        }
    }

    // =========================================================================
    //  MigracaoEngine.ProgressListener
    // =========================================================================

    @Override
    public void onLog(final String msg) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override public void run() {
                txtLog.append(msg + "\n");
                txtLog.setCaretPosition(txtLog.getDocument().getLength());
                setStatus(msg.length() > 100 ? msg.substring(0, 100) + "..." : msg);
            }
        });
    }

    @Override
    public void onStepInicio(final String nome, final int atual, final int total) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override public void run() {
                lblStep.setText("Step " + atual + "/" + total + ": " + nome);
                prgSteps.setMaximum(total);
                prgSteps.setValue(atual - 1);
                prgSteps.setString((atual - 1) + " / " + total);
            }
        });
    }

    @Override
    public void onStepConcluido(final String nome, final int ins, final int ign, final int err, final boolean ok) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override public void run() {
                int v = prgSteps.getValue() + 1;
                prgSteps.setValue(v);
                prgSteps.setString(v + " / " + prgSteps.getMaximum());
                if (!ok) {
                    lblStep.setForeground(Color.RED);
                    lblStep.setText("FALHOU: " + nome);
                }
            }
        });
    }

    @Override
    public void onConcluido(final boolean ok, final String mensagemFinal) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override public void run() {
                btnIniciar.setEnabled(true);
                btnCancelar.setEnabled(false);
                lblStep.setText(ok ? "\u2713 Concluído com sucesso!" : "\u2717 " + mensagemFinal);
                prgSteps.setValue(ok ? prgSteps.getMaximum() : prgSteps.getValue());
                prgSteps.setString(ok ? "Concluído!" : "Falhou");
                setStatus(mensagemFinal);
                JOptionPane.showMessageDialog(MainFrame.this, mensagemFinal,
                    ok ? "Migração concluída" : "Erro na migração",
                    ok ? JOptionPane.INFORMATION_MESSAGE : JOptionPane.ERROR_MESSAGE);
            }
        });
    }

    // =========================================================================
    //  Carga de UFs e Cidades
    // =========================================================================

    private void carregarEstados() {
        if (mysqlConn == null) {
            JOptionPane.showMessageDialog(this,
                "Conecte ao MySQL (Aba 1) primeiro.",
                "Sem conexão MySQL", JOptionPane.WARNING_MESSAGE);
            return;
        }
        try {
            DefaultComboBoxModel<EstadoItem> model = new DefaultComboBoxModel<EstadoItem>();
            Statement st = mysqlConn.createStatement();
            ResultSet rs = st.executeQuery(
                "SELECT id, iduf, uf, nome FROM lc_sistemas.estados ORDER BY uf");
            while (rs.next()) {
                model.addElement(new EstadoItem(
                    rs.getInt("id"), rs.getInt("iduf"),
                    rs.getString("uf"), rs.getString("nome")));
            }
            rs.close(); st.close();
            cmbUf.setModel(model);
            // Seleciona PE como padrão
            for (int i = 0; i < model.getSize(); i++) {
                if ("PE".equals(model.getElementAt(i).uf)) {
                    cmbUf.setSelectedIndex(i); break;
                }
            }
            setStatus("UFs carregadas (" + model.getSize() + " estados). Selecione a UF do cliente.");
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this,
                "Erro ao carregar estados:\n" + ex.getMessage(),
                "Erro", JOptionPane.ERROR_MESSAGE);
            setStatus("Erro ao carregar estados: " + ex.getMessage());
        }
    }

    private void carregarCidades(int iduf) {
        if (mysqlConn == null) return;
        try {
            DefaultComboBoxModel<CidadeItem> model = new DefaultComboBoxModel<CidadeItem>();
            Statement st = mysqlConn.createStatement();
            ResultSet rs = st.executeQuery(
                "SELECT id, nome FROM lc_sistemas.cidades WHERE iduf = " + iduf + " ORDER BY nome");
            while (rs.next()) {
                model.addElement(new CidadeItem(rs.getInt("id"), rs.getString("nome")));
            }
            rs.close(); st.close();
            cmbCidade.setModel(model);
            setStatus("Cidades carregadas: " + model.getSize() + " cidades.");
        } catch (Exception ex) {
            setStatus("Erro ao carregar cidades: " + ex.getMessage());
        }
    }

    // =========================================================================
    //  Montagem da configuração
    // =========================================================================

    private MigracaoConfig montarConfig() {
        EstadoItem est = (EstadoItem) cmbUf.getSelectedItem();
        CidadeItem cid = (CidadeItem) cmbCidade.getSelectedItem();
        if (est == null || cid == null) {
            JOptionPane.showMessageDialog(this,
                "Selecione UF e Cidade do cliente (Aba 3).\n" +
                "Conecte ao MySQL (Aba 1) para carregar os dados.",
                "Configuração incompleta", JOptionPane.WARNING_MESSAGE);
            tabPrincipal.setSelectedComponent(tabCliente);
            marcarNavAtivo(btnNavCliente);
            return null;
        }
        try {
            MigracaoConfig cfg = new MigracaoConfig();
            cfg.setEmpresaCnpj(txtClienteCnpj.getText().trim());
            cfg.setEmpresaId(Integer.parseInt(txtEmpresaId.getText().trim()));
            cfg.setCidadeDefaultId(cid.id);
            cfg.setEstadoDefaultId(est.id);
            cfg.setRegimeTributario(cmbRegimeTributario.getSelectedIndex() == 0 ? "SIMPLES" : "NORMAL");
            // Destino
            cfg.setMyHost(txtMyHost.getText().trim());
            cfg.setMyPorta(txtMyPorta.getText().trim());
            cfg.setMyDatabase(txtMyDatabase.getText().trim());
            cfg.setMyUsuario(txtMyUsuario.getText().trim());
            cfg.setMySenha(new String(txtMySenha.getPassword()));
            // Origem (Firebird)
            cfg.setOrigemHost(txtOrigemHost.getText().trim());
            cfg.setOrigemPorta(txtOrigemPorta.getText().trim());
            cfg.setOrigemArquivoFdb(txtOrigemArquivoFdb.getText().trim());
            cfg.setOrigemUsuario(txtOrigemUsuario.getText().trim());
            cfg.setOrigemSenha(new String(txtOrigemSenha.getPassword()));
            return cfg;
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this,
                "ID Empresa deve ser um número inteiro.",
                "Configuração inválida", JOptionPane.WARNING_MESSAGE);
            return null;
        }
    }

    // =========================================================================
    //  Helpers de URL
    // =========================================================================

    private String buildUrlMySQL() {
        return "jdbc:mysql://" + txtMyHost.getText().trim()
             + ":" + txtMyPorta.getText().trim()
             + "/" + txtMyDatabase.getText().trim()
             + "?useSSL=false&allowPublicKeyRetrieval=true"
             + "&characterEncoding=UTF-8&serverTimezone=America%2FSao_Paulo"
             + "&useOldAliasMetadataBehavior=true&allowMultiQueries=true";
    }

    private String buildUrlOrigem() {
        String fdb = txtOrigemArquivoFdb.getText().trim().replace("\\", "/");
        return "jdbc:firebirdsql://" + txtOrigemHost.getText().trim()
             + ":" + txtOrigemPorta.getText().trim()
             + "/" + fdb
             + "?encoding=ISO8859_1&charSet=ISO-8859-1";
    }

    // =========================================================================
    //  EstilosCores — idêntico ao gdoor (FlatClientProperties, sidebar escura)
    // =========================================================================

    private void aplicarEstilosCores() {
        // Esconde a barra de abas — navegação é feita pela sidebar
        tabPrincipal.setUI(new BasicTabbedPaneUI() {
            @Override
            protected int calculateTabAreaHeight(int tp, int rc, int max) { return 0; }
        });
        // Log com fundo preto (terminal)
        scrLog.getViewport().setBackground(new Color(18, 18, 18));
        txtLog.setBackground(new Color(18, 18, 18));
        txtLog.setForeground(new Color(80, 220, 100));
        prgSteps.setBackground(new Color(50, 50, 50));
        // Primeiro botão ativo = MySQL
        marcarNavAtivo(btnNavMySQL);
        tabPrincipal.setSelectedComponent(tabDestino);
    }

    private void marcarNavAtivo(JButton btn) {
        // Remove destaque do anterior
        if (btnNavAtivo != null) {
            btnNavAtivo.putClientProperty(FlatClientProperties.STYLE,
                "background: rgb(63,42,115); foreground: rgb(255,255,255);" +
                "hoverBackground: rgb(80,55,140); font: 13 SansSerif;");
        }
        // Aplica destaque no novo
        btnNavAtivo = btn;
        btn.putClientProperty(FlatClientProperties.STYLE,
            "background: rgb(92,68,164); foreground: rgb(255,255,255);" +
            "hoverBackground: rgb(110,82,190); pressedBackground: rgb(52,36,95);" +
            "font: bold 13 SansSerif;");
    }

    private void estilizarBtnNav(JButton btn) {
        btn.setBackground(new Color(63, 42, 115));
        btn.setFont(new Font("SansSerif", Font.PLAIN, 13));
        btn.setForeground(Color.WHITE);
        btn.setBorderPainted(false);
        btn.setFocusPainted(false);
        btn.setHorizontalAlignment(SwingConstants.LEFT);
    }

    public static void botaoPrimario(JButton btn) {
        btn.putClientProperty(FlatClientProperties.STYLE,
            "background: rgb(92,68,164); foreground: rgb(255,255,255);" +
            "hoverBackground: rgb(78,55,135); pressedBackground: rgb(52,36,95);");
    }

    private void setStatus(String msg) {
        lblStatus.setText(msg);
    }

    // =========================================================================
    //  Inner classes — modelo ComboBox (igual ao gdoor)
    // =========================================================================

    private static class EstadoItem {
        final int    id;
        final int    iduf;
        final String uf;
        final String nome;
        EstadoItem(int id, int iduf, String uf, String nome) {
            this.id = id; this.iduf = iduf; this.uf = uf; this.nome = nome;
        }
        @Override public String toString() { return uf + " — " + nome; }
    }

    private static class CidadeItem {
        final int    id;
        final String nome;
        CidadeItem(int id, String nome) { this.id = id; this.nome = nome; }
        @Override public String toString() { return nome; }
    }

    // =========================================================================
    //  Main
    // =========================================================================

    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(new FlatLightLaf());
            UIManager.put("Component.arc", 10);
            UIManager.put("ComboBox.arc", 10);
            UIManager.put("ToggleButton.arc", 10);
            UIManager.put("Button.arc", 10);
            UIManager.put("Dialog.arc", 10);
            UIManager.put("ScrollPane.arc", 10);
        } catch (Exception ex) {
            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
        }
        java.awt.EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() { new MainFrame().setVisible(true); }
        });
    }
}
