import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;

        try {
            serverSocket = new ServerSocket(port);

            // Allows the address/port to be reused quickly after restarting.
            serverSocket.setReuseAddress(true);

            // Wait for a client connection.
            clientSocket = serverSocket.accept();
            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            InputStream inputStream = clientSocket.getInputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());

        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
