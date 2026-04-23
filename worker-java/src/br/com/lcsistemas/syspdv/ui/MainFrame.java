package br.com.lcsistemas.syspdv.ui;

import br.com.lcsistemas.syspdv.config.MigracaoConfig;
import br.com.lcsistemas.syspdv.engine.MigracaoEngine;
import br.com.lcsistemas.syspdv.firebird.ConectorFirebirdInteligente;
import br.com.lcsistemas.syspdv.firebird.GerenciadorFirebird;
import br.com.lcsistemas.syspdv.versao.VersaoFirebird;
// import com.formdev.flatlaf.FlatClientProperties;
// import com.formdev.flatlaf.FlatLightLaf;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.SwingWorker;
import javax.imageio.ImageIO;
import javax.swing.DefaultComboBoxModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.filechooser.FileNameExtensionFilter;

/**
 * Tela principal do GDOOR Migration Engine.
 *
 * Aba 1 — Destino MySQL (lc_sistemas)
 *   Após testar conexão MySQL com sucesso, carrega UFs e Cidades nas Combos da Aba 3.
 *
 * Aba 2 — Origem Firebird (gdoor .fdb)
 *   Host, Porta 3050, caminho do .fdb via JFileChooser, usuário SYSDBA, senha.
 *   Usa driver Jaybird (org.firebirdsql.jdbc.FBDriver).
 *
 * Aba 3 — Cliente
 *   Nome, UF (JComboBox carregado de lc_sistemas.estados),
 *   Cidade (JComboBox recarregado pelo estado selecionado),
 *   ID Empresa (fixo).
 *
 * Aba 4 — Migração
 *   Log, barra de progresso, Iniciar / Cancelar.
 */
public class MainFrame extends javax.swing.JFrame
        implements MigracaoEngine.ProgressListener {

    // ── Conexão MySQL mantida para carregar combos ─────────────────────────
    private Connection mysqlConn;

    // ──Engine ──────────────────────────────────────────────────────────────
    private MigracaoEngine engine;

    // ── Caminho do .sql gerado na última migração ──────────────────────────
    private String ultimoSqlPath = "";

    // =========================================================================
    //  Construtor
    // =========================================================================

            public static void main(String args[]) {
        // FlatLaf removido por falta de JAR
        /*
        try {
            UIManager.setLookAndFeel(new FlatLightLaf());
            UIManager.put("Component.arc", 10);
            UIManager.put("ComboBox.arc", 10);
            UIManager.put("ToggleButton.arc", 10);
            UIManager.put("Button.arc", 10);
            UIManager.put("Dialog.arc", 10);
            UIManager.put("ScrollPane.arc", 10);
            UIManager.put("Button.arc", 10);
        } catch (UnsupportedLookAndFeelException ex) {
            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
        }
        */
        java.awt.EventQueue.invokeLater(() -> {
            new MainFrame().setVisible(true);
        });
        
    }
    
            
            
    public MainFrame() {
        super();
        initComponents();
        botaoPrimario(btnTestarMySQL);
        botaoPrimario(btnIniciar);
        botaoPrimario(btnTestarFb);
        botaoPrimario(btnCarregarCidades);
        aplicarEstilosCores();
        setLocationRelativeTo(null);
        setTitle("LC Sistemas — SYSPDV Engine v1.0");
        inicializarAjusteLogo();
        pararFirebirdEmBackground();
    }

    private void pararFirebirdEmBackground() {
        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() {
                GerenciadorFirebird.pararTodosServicos(msg -> {});
                return null;
            }
        }.execute();
    }
    
    


    // =========================================================================
    //  initComponents — gerado pelo GUI Builder
    // =========================================================================
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        pnlMain = new javax.swing.JPanel();
        pnlSidebar = new javax.swing.JPanel();
        lblSubtitulo = new javax.swing.JLabel();
        jSeparator1 = new javax.swing.JSeparator();
        lblSecaoConfig = new javax.swing.JLabel();
        btnNavMySQL = new javax.swing.JButton();
        btnNavFirebird = new javax.swing.JButton();
        btnNavCliente = new javax.swing.JButton();
        jSeparator2 = new javax.swing.JSeparator();
        lblSecaoExec = new javax.swing.JLabel();
        btnNavMigracao = new javax.swing.JButton();
        lblVersao1 = new javax.swing.JLabel();
        jlogo = new javax.swing.JLabel();
        tabPrincipal = new javax.swing.JTabbedPane();
        tabDestino = new javax.swing.JPanel();
        lblMyHost = new javax.swing.JLabel();
        txtMyHost = new javax.swing.JTextField();
        lblMyPorta = new javax.swing.JLabel();
        txtMyPorta = new javax.swing.JTextField();
        lblMyDatabase = new javax.swing.JLabel();
        txtMyDatabase = new javax.swing.JTextField();
        lblMyUsuario = new javax.swing.JLabel();
        txtMyUsuario = new javax.swing.JTextField();
        lblMySenha = new javax.swing.JLabel();
        txtMySenha = new javax.swing.JPasswordField();
        btnTestarMySQL = new javax.swing.JButton();
        tabOrigem = new javax.swing.JPanel();
        lblFbHost = new javax.swing.JLabel();
        txtFbHost = new javax.swing.JTextField();
        lblFbPorta = new javax.swing.JLabel();
        txtFbPorta = new javax.swing.JTextField();
        lblFbArquivo = new javax.swing.JLabel();
        txtFbArquivo = new javax.swing.JTextField();
        btnBrowseFdb = new javax.swing.JButton();
        lblFbUsuario = new javax.swing.JLabel();
        txtFbUsuario = new javax.swing.JTextField();
        lblFbSenha = new javax.swing.JLabel();
        txtFbSenha = new javax.swing.JPasswordField();
        btnTestarFb = new javax.swing.JButton();
        tabCliente = new javax.swing.JPanel();
        lblClienteNome = new javax.swing.JLabel();
        txtClienteNome = new javax.swing.JTextField();
        lblClienteUf = new javax.swing.JLabel();
        cmbUf = new javax.swing.JComboBox();
        btnCarregarCidades = new javax.swing.JButton();
        lblClienteCidade = new javax.swing.JLabel();
        cmbCidade = new javax.swing.JComboBox();
        lblRegimeTributario = new javax.swing.JLabel();
        cmbRegimeTributario = new javax.swing.JComboBox();
        lblEmpresaId = new javax.swing.JLabel();
        txtEmpresaId = new javax.swing.JTextField();
        tabMigracao = new javax.swing.JPanel();
        lblStep = new javax.swing.JLabel();
        prgSteps = new javax.swing.JProgressBar();
        scrLog = new javax.swing.JScrollPane();
        txtLog = new javax.swing.JTextArea();
        btnIniciar = new javax.swing.JButton();
        btnCancelar = new javax.swing.JButton();
        pnlStatusBar = new javax.swing.JPanel();
        lblStatus = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("LC Sistemas — GDOOR Migration Engine v1.0");
        setMinimumSize(new java.awt.Dimension(950, 620));

        pnlMain.setBackground(new java.awt.Color(37, 37, 37));
        pnlMain.setLayout(new java.awt.BorderLayout());

        pnlSidebar.setBackground(new java.awt.Color(63, 42, 115));
        pnlSidebar.setForeground(new java.awt.Color(63, 42, 115));
        pnlSidebar.setPreferredSize(new java.awt.Dimension(195, 0));

        lblSubtitulo.setBackground(new java.awt.Color(63, 42, 115));
        lblSubtitulo.setFont(new java.awt.Font("SansSerif", 0, 11)); // NOI18N
        lblSubtitulo.setForeground(new java.awt.Color(130, 130, 130));
        lblSubtitulo.setText("Migração SYSPDV");
        lblSubtitulo.setBorder(javax.swing.BorderFactory.createEmptyBorder(0, 16, 0, 10));

        jSeparator1.setBackground(new java.awt.Color(63, 42, 115));
        jSeparator1.setForeground(new java.awt.Color(63, 42, 115));

        lblSecaoConfig.setBackground(new java.awt.Color(63, 42, 115));
        lblSecaoConfig.setFont(new java.awt.Font("SansSerif", 0, 10)); // NOI18N
        lblSecaoConfig.setForeground(new java.awt.Color(255, 255, 255));
        lblSecaoConfig.setText("CONFIGURAÇÃO");
        lblSecaoConfig.setBorder(javax.swing.BorderFactory.createEmptyBorder(14, 16, 6, 10));

        btnNavMySQL.setBackground(new java.awt.Color(63, 42, 115));
        btnNavMySQL.setFont(new java.awt.Font("SansSerif", 0, 13)); // NOI18N
        btnNavMySQL.setForeground(new java.awt.Color(255, 255, 255));
        btnNavMySQL.setText("  Destino MySQL");
        btnNavMySQL.setBorderPainted(false);
        btnNavMySQL.setFocusPainted(false);
        btnNavMySQL.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnNavMySQL.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnNavMySQLActionPerformed(evt);
            }
        });

        btnNavFirebird.setBackground(new java.awt.Color(63, 42, 115));
        btnNavFirebird.setFont(new java.awt.Font("SansSerif", 0, 13)); // NOI18N
        btnNavFirebird.setForeground(new java.awt.Color(255, 255, 255));
        btnNavFirebird.setText("  Origem Firebird");
        btnNavFirebird.setBorderPainted(false);
        btnNavFirebird.setFocusPainted(false);
        btnNavFirebird.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnNavFirebird.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnNavFirebirdActionPerformed(evt);
            }
        });

        btnNavCliente.setBackground(new java.awt.Color(63, 42, 115));
        btnNavCliente.setFont(new java.awt.Font("SansSerif", 0, 13)); // NOI18N
        btnNavCliente.setForeground(new java.awt.Color(255, 255, 255));
        btnNavCliente.setText("  Cliente");
        btnNavCliente.setBorderPainted(false);
        btnNavCliente.setFocusPainted(false);
        btnNavCliente.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnNavCliente.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnNavClienteActionPerformed(evt);
            }
        });

        jSeparator2.setBackground(new java.awt.Color(63, 42, 115));
        jSeparator2.setForeground(new java.awt.Color(63, 42, 115));

        lblSecaoExec.setBackground(new java.awt.Color(63, 42, 115));
        lblSecaoExec.setFont(new java.awt.Font("SansSerif", 0, 10)); // NOI18N
        lblSecaoExec.setForeground(new java.awt.Color(255, 255, 255));
        lblSecaoExec.setText("EXECUÇÃO");
        lblSecaoExec.setBorder(javax.swing.BorderFactory.createEmptyBorder(14, 16, 6, 10));

        btnNavMigracao.setBackground(new java.awt.Color(63, 42, 115));
        btnNavMigracao.setFont(new java.awt.Font("SansSerif", 0, 13)); // NOI18N
        btnNavMigracao.setForeground(new java.awt.Color(255, 255, 255));
        btnNavMigracao.setText("  Migração");
        btnNavMigracao.setBorderPainted(false);
        btnNavMigracao.setFocusPainted(false);
        btnNavMigracao.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnNavMigracao.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnNavMigracaoActionPerformed(evt);
            }
        });

        lblVersao1.setFont(new java.awt.Font("SansSerif", 0, 10)); // NOI18N
        lblVersao1.setForeground(new java.awt.Color(101, 101, 101));
        lblVersao1.setText("v1.0");
        lblVersao1.setBorder(javax.swing.BorderFactory.createEmptyBorder(6, 16, 0, 10));

        jlogo.setIcon(new javax.swing.ImageIcon(getClass().getResource("/br/com/lcsistemas/syspdv/icon/lc_logoSofthouse_para_fundo_escuro2.png"))); // NOI18N
        jlogo.setMaximumSize(new java.awt.Dimension(150, 150));
        jlogo.setMinimumSize(new java.awt.Dimension(150, 150));

        javax.swing.GroupLayout pnlSidebarLayout = new javax.swing.GroupLayout(pnlSidebar);
        pnlSidebar.setLayout(pnlSidebarLayout);
        pnlSidebarLayout.setHorizontalGroup(
            pnlSidebarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jSeparator1)
            .addComponent(lblSecaoConfig, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jSeparator2)
            .addComponent(lblSecaoExec, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pnlSidebarLayout.createSequentialGroup()
                .addGap(0, 28, Short.MAX_VALUE)
                .addGroup(pnlSidebarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pnlSidebarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                        .addComponent(btnNavCliente, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnNavFirebird, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnNavMySQL, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 167, Short.MAX_VALUE))
                    .addComponent(btnNavMigracao, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 160, javax.swing.GroupLayout.PREFERRED_SIZE)))
            .addGroup(pnlSidebarLayout.createSequentialGroup()
                .addGroup(pnlSidebarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pnlSidebarLayout.createSequentialGroup()
                        .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addGroup(pnlSidebarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(lblVersao1, javax.swing.GroupLayout.Alignment.TRAILING)
                            .addComponent(lblSubtitulo, javax.swing.GroupLayout.Alignment.TRAILING)))
                    .addComponent(jlogo, javax.swing.GroupLayout.PREFERRED_SIZE, 0, Short.MAX_VALUE))
                .addContainerGap())
        );
        pnlSidebarLayout.setVerticalGroup(
            pnlSidebarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pnlSidebarLayout.createSequentialGroup()
                .addGap(84, 84, 84)
                .addComponent(jSeparator1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(lblSecaoConfig)
                .addGap(0, 0, 0)
                .addComponent(btnNavMySQL, javax.swing.GroupLayout.PREFERRED_SIZE, 42, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(btnNavFirebird, javax.swing.GroupLayout.PREFERRED_SIZE, 42, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(btnNavCliente, javax.swing.GroupLayout.PREFERRED_SIZE, 42, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(jSeparator2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0)
                .addComponent(lblSecaoExec)
                .addGap(0, 0, 0)
                .addComponent(btnNavMigracao, javax.swing.GroupLayout.PREFERRED_SIZE, 42, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 137, Short.MAX_VALUE)
                .addComponent(jlogo, javax.swing.GroupLayout.PREFERRED_SIZE, 53, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(lblSubtitulo)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(lblVersao1)
                .addContainerGap())
        );

        pnlMain.add(pnlSidebar, java.awt.BorderLayout.WEST);

        tabPrincipal.setFont(new java.awt.Font("SansSerif", 0, 12)); // NOI18N

        lblMyHost.setText("Host:");

        txtMyHost.setText("localhost");
        txtMyHost.setPreferredSize(null);

        lblMyPorta.setText("Porta:");

        txtMyPorta.setText("3306");
        txtMyPorta.setPreferredSize(null);

        lblMyDatabase.setText("Banco (destino):");

        txtMyDatabase.setText("lc_sistemas");
        txtMyDatabase.setPreferredSize(null);

        lblMyUsuario.setText("Usuário:");

        txtMyUsuario.setText("root");
        txtMyUsuario.setPreferredSize(null);

        lblMySenha.setText("Senha:");

        txtMySenha.setText("123456");
        txtMySenha.setPreferredSize(null);

        btnTestarMySQL.setText("Conexão MySQL");
        btnTestarMySQL.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseMoved(java.awt.event.MouseEvent evt) {
                btnTestarMySQLMouseMoved(evt);
            }
        });
        btnTestarMySQL.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnTestarMySQLActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout tabDestinoLayout = new javax.swing.GroupLayout(tabDestino);
        tabDestino.setLayout(tabDestinoLayout);
        tabDestinoLayout.setHorizontalGroup(
            tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(tabDestinoLayout.createSequentialGroup()
                .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(tabDestinoLayout.createSequentialGroup()
                        .addGap(30, 30, 30)
                        .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                            .addComponent(lblMyHost)
                            .addComponent(lblMyPorta)
                            .addComponent(lblMyDatabase)
                            .addComponent(lblMyUsuario)
                            .addComponent(lblMySenha))
                        .addGap(10, 10, 10)
                        .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(txtMyPorta, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMyHost, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMySenha, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMyUsuario, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtMyDatabase, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 407, Short.MAX_VALUE)))
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, tabDestinoLayout.createSequentialGroup()
                        .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnTestarMySQL, javax.swing.GroupLayout.PREFERRED_SIZE, 120, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addGap(12, 12, 12))
        );
        tabDestinoLayout.setVerticalGroup(
            tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(tabDestinoLayout.createSequentialGroup()
                .addGap(30, 30, 30)
                .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyHost)
                    .addComponent(txtMyHost, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(8, 8, 8)
                .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyPorta)
                    .addComponent(txtMyPorta, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(8, 8, 8)
                .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyDatabase)
                    .addComponent(txtMyDatabase, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(8, 8, 8)
                .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMyUsuario)
                    .addComponent(txtMyUsuario, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(8, 8, 8)
                .addGroup(tabDestinoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMySenha)
                    .addComponent(txtMySenha, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 300, Short.MAX_VALUE)
                .addComponent(btnTestarMySQL, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(12, 12, 12))
        );

        tabPrincipal.addTab("1 - Destino (MySQL)", tabDestino);

        lblFbHost.setText("Host:");

        txtFbHost.setText("localhost");

        lblFbPorta.setText("Porta:");

        txtFbPorta.setText("3050");

        lblFbArquivo.setText("Arquivo .fdb:");

        txtFbArquivo.setEditable(false);
        txtFbArquivo.setBackground(new java.awt.Color(255, 255, 255));

        btnBrowseFdb.setText("...");
        btnBrowseFdb.setToolTipText("Selecionar arquivo .fdb");
        btnBrowseFdb.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnBrowseFdbActionPerformed(evt);
            }
        });

        lblFbUsuario.setText("Usuário:");

        txtFbUsuario.setText("SYSDBA");

        lblFbSenha.setText("Senha:");

        txtFbSenha.setText("masterkey");

        btnTestarFb.setText("Conexão Firebird");
        btnTestarFb.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnTestarFbActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout tabOrigemLayout = new javax.swing.GroupLayout(tabOrigem);
        tabOrigem.setLayout(tabOrigemLayout);
        tabOrigemLayout.setHorizontalGroup(
            tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(tabOrigemLayout.createSequentialGroup()
                .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(tabOrigemLayout.createSequentialGroup()
                        .addGap(30, 30, 30)
                        .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                            .addComponent(lblFbHost)
                            .addComponent(lblFbPorta)
                            .addComponent(lblFbArquivo)
                            .addComponent(lblFbUsuario)
                            .addComponent(lblFbSenha))
                        .addGap(10, 10, 10)
                        .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(txtFbHost, javax.swing.GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)
                            .addComponent(txtFbPorta, javax.swing.GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)
                            .addGroup(tabOrigemLayout.createSequentialGroup()
                                .addComponent(txtFbArquivo, javax.swing.GroupLayout.DEFAULT_SIZE, 384, Short.MAX_VALUE)
                                .addGap(5, 5, 5)
                                .addComponent(btnBrowseFdb, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE))
                            .addComponent(txtFbUsuario, javax.swing.GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)
                            .addComponent(txtFbSenha, javax.swing.GroupLayout.DEFAULT_SIZE, 424, Short.MAX_VALUE)))
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, tabOrigemLayout.createSequentialGroup()
                        .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnTestarFb, javax.swing.GroupLayout.PREFERRED_SIZE, 120, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addGap(12, 12, 12))
        );
        tabOrigemLayout.setVerticalGroup(
            tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(tabOrigemLayout.createSequentialGroup()
                .addGap(30, 30, 30)
                .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblFbHost)
                    .addComponent(txtFbHost, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(8, 8, 8)
                .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblFbPorta)
                    .addComponent(txtFbPorta, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(8, 8, 8)
                .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblFbArquivo)
                    .addComponent(txtFbArquivo, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnBrowseFdb))
                .addGap(8, 8, 8)
                .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblFbUsuario)
                    .addComponent(txtFbUsuario, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(8, 8, 8)
                .addGroup(tabOrigemLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblFbSenha)
                    .addComponent(txtFbSenha, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 314, Short.MAX_VALUE)
                .addComponent(btnTestarFb, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(12, 12, 12))
        );

        tabPrincipal.addTab("2 - Origem (Firebird)", tabOrigem);

        lblClienteNome.setText("CNPJ");

        txtClienteNome.setText("14.988.683/0001-41");

        lblClienteUf.setText("UF:");

        cmbUf.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cmbUfActionPerformed(evt);
            }
        });

        btnCarregarCidades.setText("Proximo");
        btnCarregarCidades.setToolTipText("Recarregar UFs e Cidades do banco MySQL");
        btnCarregarCidades.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnCarregarCidadesActionPerformed(evt);
            }
        });

        lblClienteCidade.setText("Cidade Padrão:");

        lblRegimeTributario.setText("Regime Tributário:");

        cmbRegimeTributario.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "Simples Nacional", "Regime Normal" }));

        lblEmpresaId.setText("ID Empresa:");

        txtEmpresaId.setText("1");

        javax.swing.GroupLayout tabClienteLayout = new javax.swing.GroupLayout(tabCliente);
        tabCliente.setLayout(tabClienteLayout);
        tabClienteLayout.setHorizontalGroup(
            tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(tabClienteLayout.createSequentialGroup()
                .addGap(30, 30, 30)
                .addGroup(tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(lblClienteNome)
                    .addComponent(lblClienteUf)
                    .addComponent(lblClienteCidade)
                    .addComponent(lblRegimeTributario)
                    .addComponent(lblEmpresaId))
                .addGap(10, 10, 10)
                .addGroup(tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(txtClienteNome, javax.swing.GroupLayout.DEFAULT_SIZE, 395, Short.MAX_VALUE)
                    .addComponent(cmbCidade, 0, 395, Short.MAX_VALUE)
                    .addGroup(tabClienteLayout.createSequentialGroup()
                        .addComponent(cmbRegimeTributario, javax.swing.GroupLayout.PREFERRED_SIZE, 200, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 0, Short.MAX_VALUE))
                    .addComponent(txtEmpresaId, javax.swing.GroupLayout.DEFAULT_SIZE, 395, Short.MAX_VALUE)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, tabClienteLayout.createSequentialGroup()
                        .addGap(0, 0, Short.MAX_VALUE)
                        .addComponent(btnCarregarCidades, javax.swing.GroupLayout.PREFERRED_SIZE, 120, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(cmbUf, 0, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addGap(12, 12, 12))
        );
        tabClienteLayout.setVerticalGroup(
            tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(tabClienteLayout.createSequentialGroup()
                .addGap(30, 30, 30)
                .addGroup(tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblClienteNome)
                    .addComponent(txtClienteNome, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(12, 12, 12)
                .addGroup(tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblClienteUf)
                    .addComponent(cmbUf, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(13, 13, 13)
                .addGroup(tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblClienteCidade)
                    .addComponent(cmbCidade, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(12, 12, 12)
                .addGroup(tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblRegimeTributario)
                    .addComponent(cmbRegimeTributario, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(12, 12, 12)
                .addGroup(tabClienteLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblEmpresaId)
                    .addComponent(txtEmpresaId, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 298, Short.MAX_VALUE)
                .addComponent(btnCarregarCidades, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(12, 12, 12))
        );

        tabPrincipal.addTab("3 - Cliente", tabCliente);

        lblStep.setFont(new java.awt.Font("SansSerif", 1, 12)); // NOI18N
        lblStep.setText("Aguardando início...");

        prgSteps.setStringPainted(true);

        txtLog.setEditable(false);
        txtLog.setColumns(20);
        txtLog.setFont(new java.awt.Font("Monospaced", 0, 12)); // NOI18N
        txtLog.setRows(10);
        scrLog.setViewportView(txtLog);

        btnIniciar.setFont(new java.awt.Font("SansSerif", 1, 13)); // NOI18N
        btnIniciar.setText("▶  Iniciar");
        btnIniciar.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnIniciarActionPerformed(evt);
            }
        });

        btnCancelar.setText("■  Cancelar");
        btnCancelar.setEnabled(false);
        btnCancelar.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnCancelarActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout tabMigracaoLayout = new javax.swing.GroupLayout(tabMigracao);
        tabMigracao.setLayout(tabMigracaoLayout);
        tabMigracaoLayout.setHorizontalGroup(
            tabMigracaoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, tabMigracaoLayout.createSequentialGroup()
                .addGroup(tabMigracaoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(tabMigracaoLayout.createSequentialGroup()
                        .addContainerGap(309, Short.MAX_VALUE)
                        .addComponent(btnCancelar)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(btnIniciar, javax.swing.GroupLayout.PREFERRED_SIZE, 120, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(tabMigracaoLayout.createSequentialGroup()
                        .addGap(8, 8, 8)
                        .addGroup(tabMigracaoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(prgSteps, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addGroup(tabMigracaoLayout.createSequentialGroup()
                                .addComponent(lblStep)
                                .addGap(0, 0, Short.MAX_VALUE))
                            .addComponent(scrLog))))
                .addGap(12, 12, 12))
        );
        tabMigracaoLayout.setVerticalGroup(
            tabMigracaoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(tabMigracaoLayout.createSequentialGroup()
                .addGap(10, 10, 10)
                .addComponent(lblStep)
                .addGap(4, 4, 4)
                .addComponent(prgSteps, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(8, 8, 8)
                .addComponent(scrLog, javax.swing.GroupLayout.DEFAULT_SIZE, 423, Short.MAX_VALUE)
                .addGap(12, 12, 12)
                .addGroup(tabMigracaoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnIniciar, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnCancelar, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(12, 12, 12))
        );

        tabPrincipal.addTab("4 - Migração", tabMigracao);

        pnlMain.add(tabPrincipal, java.awt.BorderLayout.CENTER);

        pnlStatusBar.setBackground(new java.awt.Color(255, 255, 255));
        pnlStatusBar.setBorder(javax.swing.BorderFactory.createEtchedBorder());

        lblStatus.setBackground(new java.awt.Color(51, 51, 51));
        lblStatus.setText("Pronto. Conecte ao MySQL (Aba 1) para carregar UF e Cidade.");

        javax.swing.GroupLayout pnlStatusBarLayout = new javax.swing.GroupLayout(pnlStatusBar);
        pnlStatusBar.setLayout(pnlStatusBarLayout);
        pnlStatusBarLayout.setHorizontalGroup(
            pnlStatusBarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pnlStatusBarLayout.createSequentialGroup()
                .addGap(5, 5, 5)
                .addComponent(lblStatus, javax.swing.GroupLayout.PREFERRED_SIZE, 725, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        pnlStatusBarLayout.setVerticalGroup(
            pnlStatusBarLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pnlStatusBarLayout.createSequentialGroup()
                .addGap(3, 3, 3)
                .addComponent(lblStatus)
                .addGap(3, 3, 3))
        );

        pnlMain.add(pnlStatusBar, java.awt.BorderLayout.SOUTH);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pnlMain, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pnlMain, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );

        pack();
        setLocationRelativeTo(null);
    }// </editor-fold>//GEN-END:initComponents

    // =========================================================================
    //  Handlers de eventos — Aba 1
    // =========================================================================

    private void btnTestarMySQLActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnTestarMySQLActionPerformed

        String url  = buildUrlMySQL();
        String user = txtMyUsuario.getText().trim();
        String pass = new String(txtMySenha.getPassword());
        setStatus("Conectando ao MySQL...");
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
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this,
                "Falha ao conectar MySQL:\n" + ex.getMessage(),
                "Erro de Conexão", JOptionPane.ERROR_MESSAGE);
            setStatus("Erro MySQL: " + ex.getMessage());
        }
    }//GEN-LAST:event_btnTestarMySQLActionPerformed

    // =========================================================================
    //  Handlers de eventos — Aba 2
    // =========================================================================

    private void btnBrowseFdbActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnBrowseFdbActionPerformed
        JFileChooser fc = new JFileChooser();
        fc.setDialogTitle("Selecionar banco Firebird (.fdb ou .gdb)");
        fc.setFileFilter(new FileNameExtensionFilter(
            "Banco Firebird (*.fdb, *.gdb)", "fdb", "gdb"));
        fc.setAcceptAllFileFilterUsed(false);
        // Tenta abrir na última pasta usada (ou raiz C:)
        String atual = txtFbArquivo.getText().trim();
        if (!atual.isEmpty()) {
            File f = new File(atual);
            fc.setCurrentDirectory(f.getParentFile() != null ? f.getParentFile() : new File("C:\\"));
        } else {
            fc.setCurrentDirectory(new File("C:\\"));
        }
        if (fc.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            txtFbArquivo.setText(fc.getSelectedFile().getAbsolutePath());
        }
    }//GEN-LAST:event_btnBrowseFdbActionPerformed

    private void btnTestarFbActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnTestarFbActionPerformed
        final String arquivo = txtFbArquivo.getText().trim();
        if (arquivo.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                "Selecione o arquivo .fdb antes de testar.",
                "Arquivo não selecionado", JOptionPane.WARNING_MESSAGE);
            return;
        }

        // ── 1. Verificar se o driver está no classpath ────────────────────────
        try {
            Class.forName("org.firebirdsql.jdbc.FBDriver");
        } catch (NoClassDefFoundError | ClassNotFoundException e) {
            String dep = e.getMessage() != null ? e.getMessage() : "desconhecido";
            String msg =
                "<html><b>Driver Jaybird não encontrado ou dependência ausente!</b><br><br>"
              + "Classe que faltou: <code>" + dep + "</code><br><br>"
              + "<b>Solucione adicionando os JARs ao projeto (pasta lib/):</b><br>"
              + "&nbsp;&bull; <code>jaybird-4.0.10.java8.jar</code> &mdash; driver principal<br>"
              + "&nbsp;&bull; <code>connector-api-1.5.jar</code> &mdash; dependência obrigatória do Jaybird<br>"
              + "&nbsp;&bull; <code>commons-compress-*.jar</code> &mdash; suporte a compressão<br><br>"
              + "<b>Atenção:</b> Jaybird 4 suporta Firebird 2.5, 3.0, 4.0 e 5.x.<br>"
              + "Execute o script <code>setup-libs.ps1</code> e depois feche e reabra o NetBeans.</html>";
            JOptionPane.showMessageDialog(this, msg,
                "Driver Jaybird ausente", JOptionPane.ERROR_MESSAGE);
            setStatus("ERRO: Driver Jaybird ausente — veja a mensagem na tela.");
            return;
        }
        // ── 2. Montar configuração parcial do Firebird ────────────────────────
        //    (não requer MySQL nem dados do cliente — apenas os campos da Aba 2)

        final MigracaoConfig cfg = new MigracaoConfig();
        cfg.setFbHost(txtFbHost.getText().trim());
        cfg.setFbPorta(txtFbPorta.getText().trim());
        cfg.setFbArquivo(txtFbArquivo.getText().trim());
        cfg.setFbUsuario(txtFbUsuario.getText().trim());
        cfg.setFbSenha(new String(txtFbSenha.getPassword()));

        // ── 3. Rodar em background para não travar a UI ───────────────────────
        btnTestarFb.setEnabled(false);
        setStatus("Verificando serviço Firebird — aguarde...");

        new SwingWorker<String, String>() {

            @Override
            protected String doInBackground() throws Exception {
                final SwingWorker<String, String> self = this;

                GerenciadorFirebird.LogCallback logCb = new GerenciadorFirebird.LogCallback() {
                    @Override
                    public void log(String msg) {
                        publish(msg);
                    }
                };

                // a) Garantir que alguma versão do Firebird está ativa na porta configurada.
                //    GerenciadorFirebird testa todas as instalações da pasta FIREBIRD
                //    (incluindo múltiplas versões 5.x) antes de desistir.
                boolean fbAtivo = GerenciadorFirebird.garantirConectividade(cfg, logCb);
                if (!fbAtivo) {
                    throw new Exception(
                        "Não foi possível iniciar o serviço Firebird automaticamente.\n"
                        + "Verifique se a pasta FIREBIRD do projeto contém instalações válidas\n"
                        + "e se o arquivo .fdb selecionado está acessível.");
                }

                // b) Testar conexão JDBC com retry automático por incompatibilidade de ODS.
                //    Se o banco foi criado em FB4 mas o serviço iniciou em FB2.5,
                //    o retry troca para a versão correta e tenta todas as candidatas.
                return testarConexaoFirebirdComRetry(cfg, logCb);
            }

            @Override
            protected void process(List<String> chunks) {
                if (!chunks.isEmpty()) {
                    String last = chunks.get(chunks.size() - 1);
                    setStatus(last.length() > 100 ? last.substring(0, 100) + "..." : last);
                }
            }

            @Override
            protected void done() {
                btnTestarFb.setEnabled(true);
                try {
                    String info = get();
                    JOptionPane.showMessageDialog(MainFrame.this,
                        "Conexão Firebird OK!\n" + info
                        + "\n\nAvançando para a configuração do cliente...",
                        "Conexão OK", JOptionPane.INFORMATION_MESSAGE);
                    setStatus("Firebird OK — preencha os dados do cliente e inicie a migração.");
                    // Avançar para a aba Cliente (Aba 3) para o próximo passo
                    tabPrincipal.setSelectedComponent(tabCliente);
                    marcarNavAtivo(btnNavCliente);
                } catch (ExecutionException ee) {
                    // Limpeza: encerrar qualquer processo Firebird que iniciamos durante o teste.
                    // Sem isso, processos zumbis podem interferir com tentativas subsequentes.
                    GerenciadorFirebird.pararSeIniciado(new GerenciadorFirebird.LogCallback() {
                        @Override public void log(String m) { /* silencioso na falha do teste */ }
                    });
                    Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
                    String msg =
                        "<html><b>Falha ao conectar ao Firebird:</b><br><br>"
                      + escapeHtml(cause.getClass().getSimpleName() + ": " + cause.getMessage())
                      + "<br><br><b>Verifique:</b><br>"
                      + "&nbsp;&bull; O arquivo .fdb selecionado está acessível?<br>"
                      + "&nbsp;&bull; Host/Porta corretos? (padrão: localhost:3050)<br>"
                      + "&nbsp;&bull; Usuário/Senha corretos? (padrão: SYSDBA/masterkey)<br>"
                      + "&nbsp;&bull; A pasta FIREBIRD contém instalações compatíveis?</html>";
                    JOptionPane.showMessageDialog(MainFrame.this, msg,
                        "Erro de Conexão Firebird", JOptionPane.ERROR_MESSAGE);
                    setStatus("Erro Firebird: " + cause.getMessage());
                } catch (InterruptedException ie) {
                    GerenciadorFirebird.pararSeIniciado(new GerenciadorFirebird.LogCallback() {
                        @Override public void log(String m) { /* silencioso */ }
                    });
                    Thread.currentThread().interrupt();
                    setStatus("Teste de conexão cancelado.");
                }
            }
        }.execute();
    }//GEN-LAST:event_btnTestarFbActionPerformed

    // =========================================================================
    //  Handlers de eventos — Aba 3
    // =========================================================================

    private void cmbUfActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmbUfActionPerformed
        EstadoItem est = (EstadoItem) cmbUf.getSelectedItem();
        if (est != null) carregarCidades(est.iduf);
    }//GEN-LAST:event_cmbUfActionPerformed

    private void btnCarregarCidadesActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnCarregarCidadesActionPerformed
        tabPrincipal.setSelectedComponent(tabMigracao);
        marcarNavAtivo(btnNavMigracao);
    }//GEN-LAST:event_btnCarregarCidadesActionPerformed

    // =========================================================================
    //  Handlers de eventos — Aba 4
    // =========================================================================

    private void btnIniciarActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnIniciarActionPerformed
        MigracaoConfig config = montarConfig();
        if (config == null) return;

        btnIniciar.setEnabled(false);
        btnCancelar.setEnabled(true);
        txtLog.setText("");
        prgSteps.setValue(0);
        prgSteps.setMaximum(15);
        prgSteps.setString("0 / 15");
     //   lblStep.setForeground(DM_TEXTPreto);
        tabPrincipal.setSelectedComponent(tabMigracao);
        marcarNavAtivo(btnNavMigracao);

        ultimoSqlPath = config.getSqlOutputPath();
        engine = new MigracaoEngine(config);
        engine.setListener(this);
        engine.executar();
    }//GEN-LAST:event_btnIniciarActionPerformed

    private void btnCancelarActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnCancelarActionPerformed
        if (engine != null) {
            engine.cancelar();
            setStatus("Cancelando...");
        }
    }//GEN-LAST:event_btnCancelarActionPerformed
    
    public static void botaoPrimario(JButton btn) {
        /*
        btn.putClientProperty(FlatClientProperties.STYLE,
                "background: rgb(92,68,164);foreground: rgb(255,255,255);hoverBackground: rgb(78,55,135);pressedBackground: rgb(52,36,95);"
        );
        */
    }
   
    // =========================================================================
    //  Carga de UFs e Cidades do lc_sistemas
    // =========================================================================  
private void inicializarAjusteLogo() {
    // 1. Captura a imagem original gigante que você colocou no NetBeans
    ImageIcon iconOriginal = (ImageIcon) jlogo.getIcon();
    if (iconOriginal == null) {
        return; // Sai do método se não houver imagem
    }
    Image imgOriginal = iconOriginal.getImage();

    // 2. TIRA a imagem da label temporariamente!
    // Isso impede que o NetBeans estique a tela por causa da foto grande.
    jlogo.setIcon(null);

    // 3. Define o tamanho exato que a label deve ter (os 192x125 que você precisava)
    int larguraAlvo = 192;
    int alturaAlvo = 125;

    // Força o Swing a respeitar esse tamanho pequeno
    java.awt.Dimension tamanho = new java.awt.Dimension(larguraAlvo, alturaAlvo);
    jlogo.setPreferredSize(tamanho);
    jlogo.setMinimumSize(tamanho);
    jlogo.setMaximumSize(tamanho);
    jlogo.setSize(tamanho);

    // 4. Faz o cálculo da proporção IMEDIATAMENTE usando as medidas fixas
    double escala = Math.min((double) larguraAlvo / imgOriginal.getWidth(null), 
                             (double) alturaAlvo / imgOriginal.getHeight(null));

    int novaLarg = (int) (imgOriginal.getWidth(null) * escala);
    int novaAlt = (int) (imgOriginal.getHeight(null) * escala);

    // Evita erro de processamento do Java (não pode ter 0 pixels)
    if (novaLarg <= 0) novaLarg = 1;
    if (novaAlt <= 0) novaAlt = 1;

    // 5. Redimensiona a imagem e aplica de volta na label
    Image imgEscalada = imgOriginal.getScaledInstance(novaLarg, novaAlt, Image.SCALE_SMOOTH);
    jlogo.setIcon(new ImageIcon(imgEscalada));

    // 6. Centraliza a logo no espaço que sobrou
    jlogo.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
    jlogo.setVerticalAlignment(javax.swing.SwingConstants.CENTER);
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
                if (!ok) { lblStep.setForeground(Color.RED); lblStep.setText("FALHOU: " + nome); }
            }
        });
    }

    @Override
    public void onConcluido(final boolean ok, final String mensagemFinal) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override public void run() {
                btnIniciar.setEnabled(true);
                btnCancelar.setEnabled(false);
               // lblStep.setForeground(ok ? new Color(80, 220, 100) : new Color(255, 80, 80));
                lblStep.setText(ok ? "\u2713 Conclu\u00eddo com sucesso!" : "\u2717 " + mensagemFinal);
                prgSteps.setValue(ok ? prgSteps.getMaximum() : prgSteps.getValue());
                prgSteps.setString(ok ? "Conclu\u00eddo!" : "Falhou");
                setStatus(mensagemFinal);
                String msg = mensagemFinal;
                if (ok && !ultimoSqlPath.isEmpty()) {
                    msg += "\n\nArquivo gerado:\n" + ultimoSqlPath;
                }
                JOptionPane.showMessageDialog(MainFrame.this, msg,
                    ok ? "Migração concluída" : "Erro na migração",
                    ok ? JOptionPane.INFORMATION_MESSAGE : JOptionPane.ERROR_MESSAGE);
            }
        });
    }

    // =========================================================================
    //  Carga de UFs e Cidades do lc_sistemas
    // =========================================================================

    /** Lê lc_sistemas.estados e popula cmbUf. */
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
            // Seleciona BA como padrão se existir
            for (int i = 0; i < model.getSize(); i++) {
                if ("BA".equals(model.getElementAt(i).uf)) {
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

    /** Lê lc_sistemas.cidades filtrado pelo iduf do estado selecionado. */
    private void carregarCidades(int iduf) {
        if (mysqlConn == null) return;
        try {
            DefaultComboBoxModel<CidadeItem> model = new DefaultComboBoxModel<CidadeItem>();
            Statement st = mysqlConn.createStatement();
            ResultSet rs = st.executeQuery(
                "SELECT id, nome FROM lc_sistemas.cidades WHERE iduf = " + iduf
                + " ORDER BY nome");
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
        // Validação: arquivo .fdb obrigatório
        String fbArquivo = txtFbArquivo.getText().trim();
        if (fbArquivo.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                "Selecione o arquivo .fdb do SYSPDV (Aba 2).",
                "Configuração incompleta", JOptionPane.WARNING_MESSAGE);
            tabPrincipal.setSelectedComponent(tabOrigem);
            return null;
        }

        // Validação: UF e Cidade obrigatórias
        EstadoItem est = (EstadoItem) cmbUf.getSelectedItem();
        CidadeItem cid = (CidadeItem) cmbCidade.getSelectedItem();
        if (est == null || cid == null) {
            JOptionPane.showMessageDialog(this,
                "Selecione UF e Cidade do cliente (Aba 3).\n" +
                "Conecte ao MySQL (Aba 1) para carregar as listas, ou\n" +
                "certifique-se de que Cidade e UF estejam selecionadas.",
                "Configuração incompleta", JOptionPane.WARNING_MESSAGE);
            tabPrincipal.setSelectedComponent(tabCliente);
            return null;
        }

        try {
            MigracaoConfig cfg = new MigracaoConfig();

            // ── Dados do cliente/empresa ──────────────────────────────────────
            cfg.setEmpresaCnpj(txtClienteNome.getText().trim());
            cfg.setClienteUf(est.uf);
            cfg.setEmpresaId(Integer.parseInt(txtEmpresaId.getText().trim()));
            cfg.setCidadeDefaultId(cid.id);
            cfg.setEstadoDefaultId(est.id);
            cfg.setRegimeTributario(cmbRegimeTributario.getSelectedIndex() == 0 ? "SIMPLES" : "NORMAL");
            cfg.setSistema("syspdv");

            // ── Origem Firebird ───────────────────────────────────────────────
            cfg.setFbHost(txtFbHost.getText().trim());
            cfg.setFbPorta(txtFbPorta.getText().trim());
            cfg.setFbArquivo(fbArquivo);
            cfg.setFbUsuario(txtFbUsuario.getText().trim());
            cfg.setFbSenha(new String(txtFbSenha.getPassword()));

            // ── Modo SQL Output: gera TabelasParaImportacao.sql na pasta do .fdb ──
            File fdb = new File(fbArquivo);
            String sqlPath = new File(fdb.getParentFile(), "TabelasParaImportacao.sql").getAbsolutePath();
            cfg.setSqlOutputPath(sqlPath);

            // ── Destino MySQL (mantido para carregamento de combos — não usado na migração SQL) ──
            cfg.setMyHost(txtMyHost.getText().trim());
            cfg.setMyPorta(txtMyPorta.getText().trim());
            cfg.setMyDatabase(txtMyDatabase.getText().trim());
            cfg.setMyUsuario(txtMyUsuario.getText().trim());
            cfg.setMySenha(new String(txtMySenha.getPassword()));

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
             + "&useOldAliasMetadataBehavior=true";
    }

    /** URL Jaybird para Firebird usando o arquivo .fdb selecionado. */
    private String buildUrlFirebird() {
        // Formato: jdbc:firebirdsql://host:porta/caminho/completo/para/arquivo.fdb
        // charSet=Cp1252 é o nome Java correto para WIN1252 (aceito pelo Jaybird 4+)
        String arquivo = txtFbArquivo.getText().trim()
            .replace("\\", "/");   // Jaybird prefere barra normal no caminho
        return "jdbc:firebirdsql://" + txtFbHost.getText().trim()
             + ":" + txtFbPorta.getText().trim()
             + "/" + arquivo
             + "?charSet=Cp1252";
    }

    /**
     * Tenta conectar ao banco Firebird com retry automático por incompatibilidade de ODS.
     *
     * <p>Análogo ao {@code MigracaoEngine#conectarOrigemComAutoRetry}, porém retorna
     * uma string descritiva da versão detectada para exibição no diálogo de sucesso.
     *
     * <p>Se o banco exigir uma versão diferente da que está rodando (erro ODS / ISC 335544379),
     * este método solicita ao {@link GerenciadorFirebird} que troque para a versão correta
     * (testando TODAS as instalações compatíveis na pasta FIREBIRD) e retenta.
     *
     * @param cfg   Configuração com dados de conexão Firebird.
     * @param logCb Callback para mensagens de progresso exibidas na status bar.
     * @return      Descrição da versão Firebird conectada.
     * @throws Exception Se não for possível conectar após todas as tentativas.
     */
    private String testarConexaoFirebirdComRetry(MigracaoConfig cfg,
                                                  GerenciadorFirebird.LogCallback logCb)
            throws Exception {

        final int MAX_TENTATIVAS = 5;
        // Usa o mesmo formato de URL que MigracaoConfig.buildUrlFirebird():
        // charSet=Cp1252 é o nome Java correto para WIN1252 (Jaybird 4+)
        final String url = cfg.buildUrlFirebird();

        for (int t = 1; t <= MAX_TENTATIVAS; t++) {
            try {
                try (Connection c = DriverManager.getConnection(
                        url, cfg.getFbUsuario(), cfg.getFbSenha())) {

                    // Detectar versão do Firebird conectado
                    try (Statement st = c.createStatement();
                         ResultSet rs = st.executeQuery(
                             "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION')"
                           + " FROM rdb$database")) {
                        if (rs.next()) {
                            String versao = rs.getString(1);
                            logCb.log("[Teste] Conexão OK. Firebird versão: " + versao);
                            return "Versão Firebird: " + versao;
                        }
                    } catch (Exception ignorado) {
                        // versão não detectada, mas a conexão foi estabelecida
                    }
                    return "Conexão estabelecida com sucesso.";
                }
            } catch (java.sql.SQLException e) {

                VersaoFirebird versaoNecessaria =
                    GerenciadorFirebird.detectarVersaoPorODS(e.getMessage());

                if (versaoNecessaria != VersaoFirebird.DESCONHECIDA && t < MAX_TENTATIVAS) {
                    // ODS incompatível: trocar para a versão correta e retentar.
                    // GerenciadorFirebird.trocarParaVersao testa TODAS as instalações
                    // compatíveis (inclusive múltiplas versões 5.x) antes de desistir.
                    logCb.log("⚠ ODS incompatível (tentativa " + t + "/" + MAX_TENTATIVAS
                        + ") — ajustando para " + versaoNecessaria + "...");

                    boolean trocou = GerenciadorFirebird.trocarParaVersao(
                        versaoNecessaria, cfg, logCb);
                    if (!trocou) {
                        throw new Exception(
                            "O banco requer Firebird " + versaoNecessaria
                            + " mas nenhuma instalação compatível pôde ser iniciada.\n"
                            + "Erro original: " + e.getMessage());
                    }
                    logCb.log("[Teste] Retentando conexão com " + versaoNecessaria + "...");
                    // Continua o loop — próxima iteração com a versão correta

                } else {
                    // Erro genuíno (não é ODS) ou tentativas esgotadas → propaga
                    throw e;
                }
            }
        }

        throw new Exception("Não foi possível conectar ao banco Firebird após "
            + MAX_TENTATIVAS + " tentativas de ajuste automático de versão.");
    }

    /** Escapa caracteres especiais HTML em mensagens de erro exibidas em JOptionPane. */
    private static String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private void setStatus(String msg) {
        lblStatus.setText(msg);
    }

    // =========================================================================
    //  Inner classes — modelo dos ComboBoxes
    // =========================================================================

    /** Item do JComboBox de estados (UF). */
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

    /** Item do JComboBox de cidades. */
    private static class CidadeItem {
        final int    id;
        final String nome;
        CidadeItem(int id, String nome) { this.id = id; this.nome = nome; }
        @Override public String toString() { return nome; }
    }

    // =========================================================================
    //  UI — Paleta de Cores (dark mode LC Sistemas)
    //  AJUSTE: altere as constantes abaixo para mudar o tema de cores.
    // =========================================================================

    // ── PALETA DE CORES ─────────────────────────────────────────────────────
    // AJUSTE: Cores globais da interface — altere aqui para mudar o tema
//    private static final Color DM_BG      = new Color(37,  37,  37);  // fundo geral (área central escura)
//    private static final Color DM_SIDEBAR = new Color(25,  25,  25);  // sidebar e cabeçalhos dos cards
//    private static final Color DM_HOVER   = new Color(113, 111, 114); // linha divisória status bar
//    private static final Color DM_ACCENT  = new Color(255,  255,  255);   // roxo LC Sistemas (botão ativo + ícones)
//    private static final Color DM_TEXT    = new Color(255, 255, 255); // texto claro geral
//    private static final Color DM_TEXTPreto    = new Color(0, 0, 0); // texto claro geral
//    private static final Color DM_LOG     = new Color(255,  255,  255); // verde neon — texto do log
//    private static final Color CARD_BG    = new Color(248, 248, 248); // fundo branco dos cards de config
//    private static final Color CARD_FG    = new Color(30,  30,  30);  // texto escuro nos cards de config

    // AJUSTE: botão ativo na sidebar (estado de seleção)
    private javax.swing.JButton btnNavAtivo;

    /**
     * Aplica o dark mode LC Sistemas sobre a estrutura já definida no .form.
     * A estrutura (sidebar, tabs, status bar) vem do initComponents() — editável
     * diretamente no NetBeans Design View.
     *
     * AJUSTE: Cor do fundo geral → DM_BG | Sidebar → DM_SIDEBAR | Acento → DM_ACCENT
     */
    private void aplicarEstilosCores() {
        // ── Fundo geral ───────────────────────────────────────────────────
      //  pnlMain.setBackground(DM_BG);

        // ── Esconder a barra de abas do JTabbedPane em runtime ────────────
        // A navegação é feita pela sidebar; as abas ficam visíveis no Design View.
        tabPrincipal.setUI(new javax.swing.plaf.basic.BasicTabbedPaneUI() {
            @Override
            protected int calculateTabAreaHeight(int tp, int rc, int max) { return 0; }
        });
     //   tabPrincipal.setBackground(DM_BG);

        // ── Estilo dos formulários de cada aba ────────────────────────────
//        aplicarEstiloFormulario(tabDestino,  false);
//        aplicarEstiloFormulario(tabOrigem,   false);
//        aplicarEstiloFormulario(tabCliente,  false);
//        aplicarEstiloFormulario(tabMigracao, true);

        // ── Aba Migração — estilo terminal ────────────────────────────────
    //    txtLog.setBackground(new Color(18, 18, 18));
//        txtLog.setForeground(DM_LOG);
 //       txtLog.setCaretColor(DM_LOG);
        scrLog.getViewport().setBackground(new Color(18, 18, 18));
        
     //   scrLog.setBorder(javax.swing.BorderFactory.createLineBorder(new Color(60, 60, 60)));
 //       prgSteps.setForeground(DM_ACCENT);
        prgSteps.setBackground(new Color(50, 50, 50));
//        lblStep.setForeground(DM_TEXTPreto); //DM_TEXT

        // ── Botões de ação nos formulários ────────────────────────────────
//        estilizarBotao(btnTestarMySQL);
//        estilizarBotao(btnBrowseFdb);
//        estilizarBotao(btnTestarFb);
//        estilizarBotao(btnCarregarCidades);
//        estilizarBotao(btnIniciar);

        // ── Marcar primeiro botão como ativo ─────────────────────────────
        marcarNavAtivo(btnNavMySQL);
        tabPrincipal.setSelectedComponent(tabDestino);
    }

    // =========================================================================
    //  Handlers — Botões de Navegação da Sidebar
    //  AJUSTE: Para adicionar abas novas, crie btnNav* no .form e adicione
    //          o handler aqui seguindo o mesmo padrão.
    // =========================================================================

    private void btnNavMySQLActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnNavMySQLActionPerformed
        tabPrincipal.setSelectedComponent(tabDestino);
        marcarNavAtivo(btnNavMySQL);
    }//GEN-LAST:event_btnNavMySQLActionPerformed

    private void btnNavFirebirdActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnNavFirebirdActionPerformed
        tabPrincipal.setSelectedComponent(tabOrigem);
        marcarNavAtivo(btnNavFirebird);
    }//GEN-LAST:event_btnNavFirebirdActionPerformed

    private void btnNavClienteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnNavClienteActionPerformed
        tabPrincipal.setSelectedComponent(tabCliente);
        marcarNavAtivo(btnNavCliente);
    }//GEN-LAST:event_btnNavClienteActionPerformed

    private void btnNavMigracaoActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnNavMigracaoActionPerformed
        tabPrincipal.setSelectedComponent(tabMigracao);
        marcarNavAtivo(btnNavMigracao);
    }//GEN-LAST:event_btnNavMigracaoActionPerformed

    private void btnTestarMySQLMouseMoved(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_btnTestarMySQLMouseMoved
        // TODO add your handling code here:
    }//GEN-LAST:event_btnTestarMySQLMouseMoved

      //  botaoPrimario();
   
    private void marcarNavAtivo(javax.swing.JButton btn) {
       // javax.swing.JButton[] todos = {
       // };
    }

    // =========================================================================
    //  Declaração de variáveis
    // =========================================================================
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnBrowseFdb;
    private javax.swing.JButton btnCancelar;
    private javax.swing.JButton btnCarregarCidades;
    private javax.swing.JButton btnIniciar;
    private javax.swing.JButton btnNavCliente;
    private javax.swing.JButton btnNavFirebird;
    private javax.swing.JButton btnNavMigracao;
    private javax.swing.JButton btnNavMySQL;
    private javax.swing.JButton btnTestarFb;
    private javax.swing.JButton btnTestarMySQL;
    private javax.swing.JComboBox cmbCidade;
    private javax.swing.JComboBox cmbRegimeTributario;
    private javax.swing.JComboBox cmbUf;
    private javax.swing.JSeparator jSeparator1;
    private javax.swing.JSeparator jSeparator2;
    private javax.swing.JLabel jlogo;
    private javax.swing.JLabel lblClienteCidade;
    private javax.swing.JLabel lblClienteNome;
    private javax.swing.JLabel lblClienteUf;
    private javax.swing.JLabel lblEmpresaId;
    private javax.swing.JLabel lblFbArquivo;
    private javax.swing.JLabel lblFbHost;
    private javax.swing.JLabel lblFbPorta;
    private javax.swing.JLabel lblFbSenha;
    private javax.swing.JLabel lblFbUsuario;
    private javax.swing.JLabel lblMyDatabase;
    private javax.swing.JLabel lblMyHost;
    private javax.swing.JLabel lblMyPorta;
    private javax.swing.JLabel lblMySenha;
    private javax.swing.JLabel lblMyUsuario;
    private javax.swing.JLabel lblRegimeTributario;
    private javax.swing.JLabel lblSecaoConfig;
    private javax.swing.JLabel lblSecaoExec;
    private javax.swing.JLabel lblStatus;
    private javax.swing.JLabel lblStep;
    private javax.swing.JLabel lblSubtitulo;
    private javax.swing.JLabel lblVersao1;
    private javax.swing.JPanel pnlMain;
    private javax.swing.JPanel pnlSidebar;
    private javax.swing.JPanel pnlStatusBar;
    private javax.swing.JProgressBar prgSteps;
    private javax.swing.JScrollPane scrLog;
    private javax.swing.JPanel tabCliente;
    private javax.swing.JPanel tabDestino;
    private javax.swing.JPanel tabMigracao;
    private javax.swing.JPanel tabOrigem;
    private javax.swing.JTabbedPane tabPrincipal;
    private javax.swing.JTextField txtClienteNome;
    private javax.swing.JTextField txtEmpresaId;
    private javax.swing.JTextField txtFbArquivo;
    private javax.swing.JTextField txtFbHost;
    private javax.swing.JTextField txtFbPorta;
    private javax.swing.JPasswordField txtFbSenha;
    private javax.swing.JTextField txtFbUsuario;
    private javax.swing.JTextArea txtLog;
    private javax.swing.JTextField txtMyDatabase;
    private javax.swing.JTextField txtMyHost;
    private javax.swing.JTextField txtMyPorta;
    private javax.swing.JPasswordField txtMySenha;
    private javax.swing.JTextField txtMyUsuario;
    // End of variables declaration//GEN-END:variables
}
