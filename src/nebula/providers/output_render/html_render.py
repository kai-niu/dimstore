from nebula.providers.output_render.output_render_base import OutputRenderBase
from IPython.display import HTML, display

class HtmlRender(OutputRenderBase):
    
    def __init__(self, config):
        self.config = config
        self.table_style = config['table_style']

    def store_detail(self, output):
        pass

    def feature_list(self, output):
        style = self.__feature_list_style__()
        table = """<table class='nebula_feature_list'> 
                    <tr> 
                        <th>Feature</th> 
                        <th>Namespace</th>
                        <th>Tags</td>
                        <th style="min-width:350px">Parameters</th>
                        <th>Comments</th>
                        <th>Author</th>
                        <th>Date_Created</th>
                    </tr> 
                    %s 
                </table>"""
        td_inner = """<tr>
                        <td>%s</td>
                        <td>%s</td>
                        <td>%s</td>
                        <td>%s</td>
                        <td>%s</td>
                        <td>%s</td>
                        <td>%s</td>
                    </tr>"""
        row_inner = ''.join(map(lambda v: td_inner%(v.name, 
                                                    v.namespace, 
                                                    self.__tags_to_html__(v.tags),
                                                    self.__params_to_html__(v.params),
                                                    v.comment,
                                                    v.author,
                                                    "{:%d, %b %Y}".format(v.create_date)), output.values()))
        table = table%(row_inner)
        display(HTML(style + table))

    def namespace_list(self, output):
        style = """<style>
                    table.nebula_ns_list,
                    table.nebula_ns_list th,
                    table.nebula_ns_list td {
                        border: 1px solid black;
                        }
                </style>"""
        table = """<table class='nebula_ns_list'> 
                    <tr> 
                        <th>Namespace</th> 
                        <th>Feature Counts</th> 
                    </tr> 
                    %s 
                </table>"""
        td_inner = """<tr>
                        <td style="text-align: left;">%s</td>
                        <td>%s</td>
                    </tr>"""
        summary_inner = '<br/><b>Total Features: %d</b>'%(sum(output[1]))

        str_namespace_list = list(map(lambda ns:'.'.join([t[1] for t in ns ]), output[0]))
        row_inner = ''.join(map(lambda item: td_inner%(item[0],item[1]),zip(str_namespace_list, output[1])))
        table = table%(row_inner) 
        display(HTML(style+table+summary_inner))

    def __tags_to_html__(self, tags):
        if tags == None or len(tags) == 0:
            return ''
        else:
            ul = """<ul class='nebula_tags'>
                    %s
                </ul>"""
            li_inner = """<li class='nebula_tag'>%s</li>"""
            li_inner = ''.join(map(lambda t: li_inner%(t), tags))
            return ul%(li_inner)

    def __params_to_html__(self, params):
        if params == None or len(params) == 0:
            return ''
        else:
            dl = """<dl>
                    %s
                </dl>"""
            dl_inner = """<dt>%s:</dt><dd>%s</dd>"""
            dl_inner = ''.join(map(lambda p: dl_inner%(p[0],p[1]), params.items()))
            return dl%(dl_inner)

    def __feature_list_style__(self):
        style = """
            <style>
                table.nebula_feature_list,
                table.nebula_feature_list th,
                table.nebula_feature_list td {
                    border: 1px solid black;
                    text-align: left;
                    }
                table.nebula_feature_list th{
                    text-align: center;
                }
                .nebula_tags {
                list-style: none;
                margin: 0;
                overflow: hidden; 
                padding: 0;
                }

                .nebula_tags li {
                float: left; 
                }

                .nebula_tag {
                background: crimson;
                border-radius: 3px 3px 3px 3px;
                color: #fff;
                display: inline-block;
                padding: 0 5px 0 5px;
                position: relative;
                margin: 5px 5px 0 0;
                text-decoration: none;
                -webkit-transition: color 0.2s;
                }

            </style>
        """

        return style

