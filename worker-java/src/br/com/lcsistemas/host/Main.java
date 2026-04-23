package br.com.lcsistemas.host;

import br.com.lcsistemas.host.ui.MainFrame;
import com.formdev.flatlaf.FlatLightLaf;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

/**
 * Ponto de entrada do HOST Migration Engine.
 * Inicia no Event Dispatch Thread do Swing.
 */
public class Main {

    public static void main(String[] args) {
        // 1. Aplica o look-and-feel do FlatLaf ANTES de abrir qualquer tela
        try {
            UIManager.setLookAndFeel(new FlatLightLaf());
            UIManager.put("Component.arc", 10);
            UIManager.put("ComboBox.arc", 10);
            UIManager.put("ToggleButton.arc", 10);
            UIManager.put("Button.arc", 10);
            UIManager.put("Dialog.arc", 10);
            UIManager.put("ScrollPane.arc", 10);
        } catch (Exception e) {
            System.err.println("Erro ao inicializar o tema FlatLaf: " + e.getMessage());
        }

        // 2. Abre a tela principal
        SwingUtilities.invokeLater(() -> new MainFrame().setVisible(true));
    }
}
