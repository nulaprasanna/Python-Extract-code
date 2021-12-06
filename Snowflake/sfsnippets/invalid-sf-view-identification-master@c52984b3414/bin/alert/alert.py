from jinja2 import Environment, FileSystemLoader
from datetime import datetime
from dateutil.tz import tzlocal
import smtplib
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import logging
# from markdown2 import Markdown
from pathlib import Path
import sys
from os import path


class Alert(object):
    '''
    This class allows you to send email using the jinja template.
    To use, you will need to extend the base.html template.
    Also allows you to embed markdown file in your template.

    Dir structure:
        The package should be in the same dir as your executable script. bin/Alert
        All the html template shoud be in Alert/html 
        All the md template should be in Alert/markdown
    Example simple usage without extension:
        alert = Alert("Test Email Subject")
        alert.render_template()
        alert.send_email(['your_username@cisco.com'])
    Example simple usage with extension:
        alert = Alert("Test Email Subject")
        alert.extend_template("temp/my_jina_template.html")
        alert.render_template(dict_template_variables)
        alert.send_email(['your_username@cisco.com'])
    '''
    def __init__(self, subject):
        self._path = path.dirname( path.abspath(__file__))
        self._content = []
        self._is_extended = False
        self.subject = subject
        self._template = self._add_template(self._path+'/html/base.html')
        self._email_body = MIMEText('', 'html')
        #this is to get ready for SMTP image
        filename = self._path+'/images/header.png'
        fp = open(filename, 'rb')
        msg_img = MIMEImage(fp.read())
        fp.close()
        msg_img.add_header('Content-ID', '<image0>')
        self._images = [msg_img]      
    
    def _add_template(self, template):
        path = Path(template)
        file_name = path.name
        path = path.parent
        templateLoader = FileSystemLoader(searchpath=path) 
        templateEnv = Environment(loader=templateLoader)
        return templateEnv.get_template(file_name)

    def _export_csv(self,df):
        import io
        with io.StringIO() as buffer:
            df.to_csv(buffer)
            return buffer.getvalue()

    '''
    This method extend the base.html.
    The given template must use {% extend "base.html"%} is the first tag in the child template.
    Otherwise error is raise.
    '''
    def extend_template(self, template):     
        with open(template) as f:
            if not  "{% extends 'base.html' %}" in f.readline().replace('"',"'"):
                raise Exception('Template not properly extended')
        self._template = self._add_template(template)
        self._is_extended = True

    '''
    This methods allow you to add header in your template
    Optional LOGO next to header
    '''
    def add_header(self,header,logo_path=''):
        template = self._add_template(self._path+'/html/header_logo.html')
        cid=''
        if logo_path:
            fp = open(logo_path, 'rb')
            msg_img = MIMEImage(fp.read())
            fp.close()
            cid = f'cid:image111'
            msg_img.add_header('Content-ID', '<image'+str(111)+'>')
            self._images.append(msg_img)
        template_vars = {'header': header,'image':cid}
        header = template.render(template_vars)
        self._content.append(header)
        return header


    '''
    Easy to template your email without html knowledge
    This method will convert md file into jinja render ready format
    and parse the python variable passed as dictionary
    Note: This is only working with extended html version
    '''
    def add_markdown(self,md_file,dict_object={}):
        # path = Path(md_file)
        # with open(path) as f:
        #     all_content = f.readlines()
        # markdowner = Markdown()
        # content = ''.join(all_content)
        # for k,v in dict_object.items():
        #     if isinstance(v,str):
        #         dict_object[k] = v.replace('_', '\_')
        # content = content.format(**dict_object)
        # content = markdowner.convert(content)
        # return content
        pass
    '''
    This method is used to generate csv file and attach in the email.
    TODO: multiple attachs
    '''
    def add_attachment(self,df,filename='attachment.csv'):
        attachment = MIMEApplication(self._export_csv(df))
        attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(filename)
        self._attachment = attachment

    '''
    This method allow pandas dataframe to be ready to be added in the template
    return: html format of dataframe
    '''
    def add_table(self,df,max_row=5,max_cols=5):
        import pandas as pd
        pd.set_option('colheader_justify', 'center')
        html = df.to_html(classes='mystyle',max_cols=max_cols,border=None,max_rows=max_row,index=False)
        template = self._add_template(self._path+'/html/table.html')
        template_vars = {'table':    html}
        self._content.append(template.render(template_vars))
        return html

    '''
    This method allows you to add a paragraph with optional header.
    Return html paragraph element
    '''
    def add_paragraph(self,paragraph,subheading=''):
        template = self._add_template(self._path+'/html/paragraph.html')
        template_vars = {'subheading': subheading, 'paragraph':paragraph}
        paragraph = template.render(template_vars)
        self._content.append(paragraph)
        return paragraph

    '''
    This method return list of images as content ids (cid).
    Note: images will show up according the same order as added
    dao header is mandantory
    embed image as src="cid:image{i}" where i >= 1 in html file
    Return: a list of cids to be reference in html image tag
    '''
    def add_images(self,list_of_images=[]):
        msg_images = []
        cids = []
        #check if the path is valid
        for z, filename in enumerate(list_of_images):
            count = len(self._images) + z
            fp = open(filename, 'rb')
            msg_img = MIMEImage(fp.read())
            fp.close()
            cid = f'cid:image{count}'
            cids.append(cid)
            msg_img.add_header('Content-ID', '<image'+str(count)+'>')
            msg_images.append(msg_img)

            template = self._add_template(self._path+'/html/image.html')
            template_vars = {'image': cid}
            image = template.render(template_vars)
            self._content.append(image)
        self._images = self._images + msg_images
        return cids

    '''
    This method return html template
    '''
    def render_template(self, dict_object={}):
        if hasattr(self,'_header'):
            dict_object['header'] = self._header
        if self._is_extended:
            output = self._template.render(dict_object)
        else:
            output = self._template.render(content = ' '.join(self._content))
        self._email_body = MIMEText(output, 'html')

    '''
    This method send email using SMTP.
    Input: list of email addresses
    '''
    def send_email(self, tolist):
        try:
            msg = MIMEMultipart()
            msgfrom = 'no-reply@cisco.com'
            msg['Subject'] = self.subject
            msg['From'] = msgfrom
            msg['To'] = ','.join(tolist)
            msg.attach(self._email_body)
            if hasattr(self, '_attachment'):
                msg.attach(self._attachment )
            for msg_img in self._images:
                msg.attach(msg_img)
            s = smtplib.SMTP("outbound.cisco.com")
            s.sendmail(msgfrom, tolist, msg.as_string())
            s.quit()
            logging.info(
                f"Mail Sent Successfully \
                {datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')}")
        except smtplib.SMTPRecipientsRefused as smtp_err:
            e = f"ERROR: {datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')} Email Scheduler is unable to send email to {tolist} refused to get emails."
            raise Exception (e)
        except Exception as e:
            e = f"ERROR: Unable to send email at \
                {datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')} to {tolist}"
            raise Exception (e)

if __name__ == "__main__":
    '''
    For testing run python alert.py
    '''
    import test
    test.run_test()
    
    

