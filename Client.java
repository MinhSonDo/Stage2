import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Client {
    public static void main(String[] args) {

        clientServerCommunication();

    }

    // This method saves all strings generated from GETS Capable command into a list
    // of strings
    public static List<String> saveAllServers(BufferedReader in, int numberOfServer) throws IOException {
        List<String> allCapableServer = new ArrayList<String>();
        while (numberOfServer != 0) {
            allCapableServer.add(in.readLine());

            numberOfServer--;
        }
        return allCapableServer;
    }

    // This method compares the cores of all capable servers. The server with the
    // highest number of cores will be saved into a string.
    // In addition, this method will count the number of the largest servers.
    // This method will return a string containing the largest server type and the
    // number of the largest servers
    public static String firstServer(DataOutputStream dout, BufferedReader in, int saveCount,
        int numberOfCapableServers, int largestCore, String largestType) {

       
        List<String> temp = new ArrayList<String>();

        try {
            dout.write(("OK\n").getBytes());

            temp = saveAllServers(in, numberOfCapableServers);


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return temp.get(0).split(" ")[0];
    }

    // This method is in charge of sending initial messages like "HELO" and "AUTH"
    public static void initialMessage(DataOutputStream dout, BufferedReader in) {

        try {
            dout.write(("HELO\n").getBytes());
            dout.flush();
            in.readLine();
            dout.write(("AUTH minh\n").getBytes());
            dout.flush();
            in.readLine();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    // This method closes data output stream, buffered reader and socket.
    public static void closeConnection(DataOutputStream dout, BufferedReader in, Socket s) {

        try {
            dout.close();
            in.close();
            s.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    // This method combines all other helper functions to communicate and schedule
    // jobs from ds-server
    public static void clientServerCommunication() {
        try {
            Socket s = new Socket("localhost", 50000);
            DataOutputStream dout = new DataOutputStream(s.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            initialMessage(dout, in);
            requestHandler(dout, in);
            closeConnection(dout, in, s);

        } catch (Exception e) {

        }

    }

    // This method is responsible for communicating with ds-server after the
    // intitial messages.
    // It also contains the LRR algorithm.
    public static void requestHandler(DataOutputStream dout, BufferedReader in) {

        String JOBN = "";
        int serverId = 0;
        int largestServerCount = 0;
        String largestType = "";
        Boolean flag = false;

        while (!(JOBN == null) && !(JOBN.contains("NONE"))) {
            try {
                dout.write(("REDY\n").getBytes());
                dout.flush();
                JOBN = in.readLine();

                if (!(JOBN == null) && JOBN.contains("JOBN")) {

                    String JOBNArray[] = JOBN.split(" ");
                    String core = "";
                    String memory = "";
                    String disk = "";
                    if (JOBNArray.length > 6) {
                        core = JOBNArray[4];
                        memory = JOBNArray[5];
                        disk = JOBNArray[6];
                    }
                    // execute this GETS code only once
                    if (!core.isEmpty() && flag == false) {
                        dout.write(("GETS Capable " + core + " " + memory + " " + disk + "\n").getBytes());

                    }
                    // execute this GETS part only once
                    if (flag == false) {
                        String dataFromGets = in.readLine();
                        dout.flush();
                        String dataFromGetsInArrayForm[] = dataFromGets.split(" ");
                        int dataCount = Integer.parseInt(dataFromGetsInArrayForm[1]);
                        int largestCore = 0;
                        int compareCore = 0;
                        String largestServerType = "";

                        String largestServerType2 = firstServer(dout, in, largestCore, dataCount, compareCore,
                                largestServerType);
                        // split the string to extract the number of the largest servers and the largest
                        // server type
                      
                        largestType = largestServerType2;

                    }
                    String temp[] = JOBN.split(" ");

                    if (serverId >= largestServerCount)
                        serverId = 0;
                    if (JOBN.length() > 0) {
                        if (flag == false) {
                            dout.write(("OK\n").getBytes());
                            dout.flush();
                            in.readLine();
                            // GETS command has done executing so set flag to true
                            flag = false;
                        }

                        dout.write(("SCHD " + temp[2] + " " + largestType + " " + serverId + "\n").getBytes());
                        dout.flush();
                        serverId++;

                    }

                    in.readLine();

                }

            } catch (Exception e) {

            }

        }
        try {
            dout.write(("QUIT\n").getBytes());
            dout.flush();

        }

        catch (Exception e) {
        }

    }

}
