package framework;

import org.apache.commons.io.IOUtils;

public class PythonLauncher {

    /**
     * Launches a Python program. The arguments passed to the function are
     * @param args
     * @throws Exception
     */
    public void launch(String... args) throws Exception{
        String[] arguments = new String[args.length+1];
        arguments[0] = "python3";
        for (int i=0; i< args.length;i++){
            arguments[i+1] = args[i];
        }
        ProcessBuilder processBuilder = new ProcessBuilder(arguments);
        processBuilder.redirectErrorStream(true);

        System.out.println("Launching Python program ...");
        Process proc = processBuilder.start();
        String res = IOUtils.toString(proc.getInputStream());
        System.out.println(res);
        int exitCode = proc.waitFor();

    }
}
