package br.com.lcsistemas.gdoor.firebird;

import br.com.lcsistemas.gdoor.versao.VersaoFirebird;

import java.io.File;

/**
 * Representa uma instalação manual do Firebird encontrada na pasta FIREBIRD do projeto.
 *
 * <p>Suporta dois layouts de instalação:
 * <ul>
 *   <li><b>Firebird 2.x</b> — executável em {@code <raiz>/bin/fbserver.exe}</li>
 *   <li><b>Firebird 3.x / 4.x / 5.x</b> — executável em {@code <raiz>/firebird.exe}</li>
 * </ul>
 *
 * <p>Instâncias criadas e ordenadas pelo {@link GerenciadorFirebird}.
 */
public class FirebirdInstalacao {

    /**
     * Layout da instalação — determina qual executável usar e como iniciá-lo.
     */
    public enum Layout {
        /** Firebird 2.x — executável em bin/fbserver.exe */
        BIN_SUPER,
        /** Firebird 3.x, 4.x, 5.x — executável em firebird.exe na raiz */
        ROOT_THREAD
    }

    private final String         nomePasta;
    private final File           raizDir;
    private final File           exeFile;
    private final String         versaoStr;
    private final VersaoFirebird versaoEnum;
    private final Layout         layout;
    private final int            porta;
    private final int            prioridade;

    public FirebirdInstalacao(String nomePasta,
                              File raizDir,
                              File exeFile,
                              String versaoStr,
                              VersaoFirebird versaoEnum,
                              Layout layout,
                              int porta,
                              int prioridade) {
        this.nomePasta  = nomePasta;
        this.raizDir    = raizDir;
        this.exeFile    = exeFile;
        this.versaoStr  = versaoStr;
        this.versaoEnum = versaoEnum;
        this.layout     = layout;
        this.porta      = porta;
        this.prioridade = prioridade;
    }

    // ── Getters ───────────────────────────────────────────────────────────────

    /** Nome da pasta (ex: "Firebird-2.5.9.manual"). */
    public String getNomePasta()           { return nomePasta; }

    /** Diretório raiz da instalação. */
    public File getRaizDir()               { return raizDir; }

    /** Arquivo executável do servidor (fbserver.exe ou firebird.exe). */
    public File getExeFile()               { return exeFile; }

    /** String da versão extraída do nome da pasta (ex: "2.5.9", "3.0.13"). */
    public String getVersaoStr()           { return versaoStr; }

    /** Versão mapeada para o enum {@link VersaoFirebird}. */
    public VersaoFirebird getVersaoEnum()  { return versaoEnum; }

    /** Layout da instalação (BIN_SUPER para 2.x, ROOT_THREAD para 3.x+). */
    public Layout getLayout()              { return layout; }

    /**
     * Porta TCP configurada para esta instalação (lida do firebird.conf).
     * Default: 3050.
     */
    public int getPorta()                  { return porta; }

    /**
     * Prioridade de tentativa (menor = tenta primeiro).
     * Baseada na compatibilidade com bancos GDOOR (FB 2.5 = prioridade 1).
     */
    public int getPrioridade()             { return prioridade; }

    @Override
    public String toString() {
        return nomePasta + " [" + versaoStr + " | porta:" + porta + " | exe:" + exeFile.getName() + "]";
    }
}
