package com.citi.purgeFramework.utils;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import javax.mail.Address;

public class SendEmail

{

    public void sendReport(Map<String, String> mail_config){
        String from = "donotreply@citi.com";
        String host = "localhost";//or IP address

        //Get the session object
        Properties properties = System.getProperties();
        String[] to = mail_config.get("results_sent_to").split(",");
        System.out.println("Inside email routine: Send To: "+mail_config.get("results_sent_to"));
        properties.setProperty("mail.smtp.host", host);
        Session session = Session.getDefaultInstance(properties);
        ProcessBuilder processBuilder = new ProcessBuilder();

        //compose the message
        try{

            //------Populate subject and body of email-------
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(from));

            InternetAddress[] mailAddress_TO = new InternetAddress [to.length] ;
            for(int i=0;i<to.length;i++){
                mailAddress_TO[i] = new InternetAddress(to[i]);
            }
            message.addRecipients(Message.RecipientType.TO, mailAddress_TO);
            //message.addRecipient(Message.RecipientType.TO,new InternetAddress(to));
            String mail_sub = mail_config.get("mail_sub");
            for(HashMap.Entry<String, String> val : mail_config.entrySet())
            {
                if (val.getValue() != null) {
                    mail_sub = mail_sub.replaceAll("<"+val.getKey()+">",val.getValue());
                }
            }
            message.setSubject(mail_sub);
            /*message.setSubject(mail_config.get("mail_sub")
                    .replace("<dataset_name>", mail_config.get("dataset_name"))
                    .replace("<date>", mail_config.get("run_date"))
                    .replace("<jobStatus>", mail_config.get("jobStatus"))
                    .replace("<env>", mail_config.get("env")));*/

            BodyPart messageBodyPart1 = new MimeBodyPart();
            String mail_body = mail_config.get("mail_body");
            for(HashMap.Entry<String, String> val : mail_config.entrySet())
            {
                if (val.getValue() != null) {
                    mail_body = mail_body.replaceAll("<"+val.getKey()+">",val.getValue());
                }
            }
            messageBodyPart1.setText(mail_body);

            System.out.println("Inside Mail routine: Exec Mode: "+mail_config.get("exec_mode"));
            System.out.println("Mail Subject: "+ mail_sub);
            /*messageBodyPart1.setText(mail_config.get("mail_body")
                    .replace("<dataset_name>", mail_config.get("dataset_name"))
                    .replace("<env>", mail_config.get("env"))
                    .replace("<app_id>", mail_config.get("appId"))
                    .replace("<actual_sql_or_input_file>", mail_config.getOrDefault("input_sql_or_file",""))
                    .replace("<tot_recs>", mail_config.getOrDefault("tot_row_count",""))
                    .replace("<no_of_passed_rules>", mail_config.getOrDefault("succeeded_constraints_cnt",""))
                    .replace("<no_of_failed_rules>", mail_config.getOrDefault("failed_constraints_cnt",""))
                    .replace("<log_path>", mail_config.get("spark_log_path")));*/
            System.out.println("---Inside email routine and the execution mode is: default. Length: "+mail_config.get("attachment_names").length());

                    //--check if there is any attachment---
            if(mail_config.get("attachment_names") != null && !mail_config.get("attachment_names").trim().isEmpty()) {
                    //if (attachmentNames.length > 0) {
                        String[] attachmentNames = mail_config.get("attachment_names").split(",");
                        String attachmentName = "";
                        System.out.println("Attachment Names:"+mail_config.get("attachment_names")+" Attachment array length "+attachmentNames.length);

                        Multipart multipart = new MimeMultipart();
                        multipart.addBodyPart(messageBodyPart1); //message content
                        String attachmentNamesToRemove="";

                        for (String value : attachmentNames) {
                            attachmentName = value.split("/")[value.split("/").length - 1];
                            System.out.println("---Inside email routine and the execution mode is: default. Extracting attachment: "+attachmentName);

                            //download the file from hdfs
                            //extract the hdfs path from provided configuration and search for part files
                            if (attachmentName.contains(".csv")) {
                                System.out.println("hdfs dfs -get " + value + "/part*.csv");
                                processBuilder.command("bash", "-c", "hdfs dfs -get ".concat(value).concat("/part*.csv"));
                            }
                            else { //To handle Excel attachments as they get saved as .xls without part files
                                System.out.println("hdfs dfs -get " + value);
                                processBuilder.command("bash", "-c", "hdfs dfs -get ".concat(value));
                            }

                            Process dwnld_process = processBuilder.start();

                            int dwnld_exitVal = dwnld_process.waitFor();
                            if (dwnld_exitVal == 0) {

                                //Rename the file
                                int rename_exitVal = 1;
                                Process rename_process = null;
                                if (attachmentName.contains("csv")) {
                                    processBuilder.command("bash", "-c", "mv part*.csv ".concat(attachmentName));
                                    rename_process = processBuilder.start();
                                    rename_exitVal = rename_process.waitFor();
                                }
                                else{ 
                                    /*
                                    this section is for excel attachment, setting the value to directly 0 as file rename is not required.
                                    hadoop will directly store as xls file(no part files will be generated). This section might need changes for 
                                    handling other file types
                                    */
                                    rename_exitVal=0;
                                }
                                if (rename_exitVal == 0) {
                                    //attach file
                                    DataSource source = new FileDataSource(attachmentName);
                                    MimeBodyPart dwnld_messageBodyPart = new MimeBodyPart();
                                    dwnld_messageBodyPart.setDataHandler(new DataHandler(source));
                                    dwnld_messageBodyPart.setFileName(attachmentName);
                                    multipart.addBodyPart(dwnld_messageBodyPart);

                                    message.setContent(multipart);

                                    if (attachmentNamesToRemove == "") {
                                        attachmentNamesToRemove = attachmentName;
                                    }
                                    else {
                                        attachmentNamesToRemove = attachmentNamesToRemove + " " + attachmentName;
                                    }
                                } else {
                                    System.out.println("Failed to rename the file downloaded from hdfs - " + value);
                                    BufferedReader stdError = new BufferedReader(new InputStreamReader(rename_process.getErrorStream()));
                                    String errMsg = null;
                                    while ((errMsg = stdError.readLine()) != null) {
                                        System.out.println("File rename Error Message: " + errMsg);
                                    }
                                }
                            } else {
                                //Failed to download file from hdfs
                                System.out.println("Failed to download the file from hdfs for " + value);
                                BufferedReader stdError = new BufferedReader(new InputStreamReader(dwnld_process.getErrorStream()));
                                String errMsg = null;
                                while ((errMsg = stdError.readLine()) != null) {
                                    System.out.println("hdfs file download failed - Error Message: " + errMsg);
                                    throw new IOException("Failed to download the DQ report file from hdfs");
                                }
                            }
                        } // end of for loop --> for (String value : attachmentNames)

                        Transport.send(message); //--Send email with all the required attachments
                        System.out.println(" ------ Mail sent for following execution mode: " + mail_config.get("exec_mode"));

                        // Remove the file after attaching to mail
                        // This is an optional module. As this activity will be taken care spark. So just setting only warning
                        processBuilder.command("bash", "-c", "rm ".concat(attachmentNamesToRemove));
                        Process remove_process = processBuilder.start();
                        int remove_exitVal = remove_process.waitFor();
                        if (remove_exitVal != 0) {
                            System.out.println("WARN: Could not remove the file(s) downloaded from hdfs: " + attachmentNamesToRemove);
                        }
                        else
                        {
                            System.out.println("INFO: Removed the following mail attachment file(s) downloaded from hdfs: " + attachmentNamesToRemove);
                        }


                    } // end of "if (attachmentNames.length > 0)" condition
            else
                    {
                        //--No attachments provided, send out a normal email
                        System.out.println("---Inside email routine and the execution mode is: default (without attachment)");
                        Multipart multipart = new MimeMultipart();
                        multipart.addBodyPart(messageBodyPart1);
                        message.setContent(multipart );
                        Transport.send(message);
                        System.out.println(" ------ Mail sent for following execution mode: " + mail_config.get("exec_mode"));
                    } // end of "else (attachmentNames.length > 0)" condition
            }
        catch (IOException e) {
            e.printStackTrace();
            mailRoutineError("Failure in sendReport email Routine.\n\nPlease check spark yarn log for further details\n\nspark application id can be fetched from Autosys log",  "Failure in sendReport email Routine: Unable to send mail",  mail_config.get("results_sent_to"));
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            mailRoutineError("Failure in sendReport email Routine.\n\nPlease check spark yarn log for further details\n\nspark application id can be fetched from Autosys log",  "Failure in sendReport email Routine: Unable to send mail",  mail_config.get("results_sent_to"));
        }
        catch (MessagingException ex) {
            ex.printStackTrace();
            mailRoutineError("Failure in sendReport email Routine.\n\nPlease check spark yarn log for further details\n\nspark application id can be fetched from Autosys log",  "Failure in sendReport email Routine: Unable to send mail",  mail_config.get("results_sent_to"));
        }
    }


    //------------------------------------------Error Email-----------------------------------------------

    public void errorMail(Map<String, String> mail_config, String err_msg){

        System.out.println("Inside error email routine");
        String from = "donotreply@citi.com";
        String host = "localhost";//or IP address

        //Get the session object
        Properties properties = System.getProperties();
        String[] to = mail_config.get("results_sent_to").split(",");
        properties.setProperty("mail.smtp.host", host);
        Session session = Session.getDefaultInstance(properties);

        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(from));
            InternetAddress[] mailAddress_TO = new InternetAddress [to.length] ;
            for(int i=0;i<to.length;i++){
                mailAddress_TO[i] = new InternetAddress(to[i]);
            }
            message.addRecipients(Message.RecipientType.TO, mailAddress_TO);

            String mail_sub = mail_config.get("err_mail_sub");
            for(HashMap.Entry<String, String> val : mail_config.entrySet())
            {
                if (val.getValue() != null) {
                    mail_sub = mail_sub.replaceAll("<"+val.getKey()+">",val.getValue());
                }
            }
            message.setSubject(mail_sub);
            /*
            message.setSubject(mail_config.get("err_mail_sub")
                    .replace("<dataset_name>", mail_config.get("dataset_name"))
                    .replace("<date>", mail_config.get("run_date"))
                    .replace("<jobStatus>", "FAILED")
                    .replace("<env>", mail_config.get("env")));
            */

            BodyPart messageBodyPart1 = new MimeBodyPart();

            String mail_body = mail_config.get("err_mail_body");
            for(HashMap.Entry<String, String> val : mail_config.entrySet())
            {
                if (val.getValue() != null) {
                    mail_body = mail_body.replaceAll("<"+val.getKey()+">",val.getValue());
                }
            }
            messageBodyPart1.setText(mail_body+"\n\n"+err_msg);

            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart1);

            message.setContent(multipart );

            Transport.send(message);

            System.out.println("message sent....");
        }
        catch (MessagingException ex) {ex.printStackTrace();
            System.out.println("---Failure in errorMail Routine: Unable to send mail---");
            mailRoutineError("Failure in errorMail Routine" +
                    ".\n\nPlease check spark yarn log for further details\n\nspark application id can be fetched from Autosys log",  "Failure in errorMail Routine: Unable to send mail",  mail_config.get("results_sent_to"));
        }
    }

    void mailRoutineError(String errmsg, String sub, String sendto){
        System.out.println("Inside localError routine");
        String from = "donotreply@citi.com";
        String host = "localhost";//or IP address

        //Get the session object
        Properties properties = System.getProperties();
        String[] to = sendto.split(",");
        properties.setProperty("mail.smtp.host", host);
        Session session = Session.getDefaultInstance(properties);

        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(from));
            InternetAddress[] mailAddress_TO = new InternetAddress [to.length] ;
            for(int i=0;i<to.length;i++){
                mailAddress_TO[i] = new InternetAddress(to[i]);
            }
            message.addRecipients(Message.RecipientType.TO, mailAddress_TO);

            String mail_sub = sub;
            message.setSubject(mail_sub);
            BodyPart messageBody = new MimeBodyPart();
            messageBody.setText(errmsg);
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBody);

            message.setContent(multipart );
            Transport.send(message);
        }
        catch(MessagingException ex) {ex.printStackTrace();
            System.out.println("---Failure in mailRoutineError : Unable to send mail---");
        }
    }

}
