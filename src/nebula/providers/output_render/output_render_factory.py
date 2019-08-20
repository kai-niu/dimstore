"""
    output render factory
"""

from nebula.providers.output_render.html_render import HtmlRender

class MetaManagerFactory():

    def __init__(self, config):
        self.config = config

    # output render factory
    def get_output_render(self):
        if self.config['output_render'] == 'html_render':
            return HtmlRender(self.config['output_render']['html_render'])

    # return supported meta manager info
    def info(self):
        return ['output_renders: html render.']