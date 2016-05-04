# 
# from logging import Formatter, getLogger
# 
# class GnanaLogFormatter(Formatter):
#     def __init__(self, **kwargs):
#         super(GnanaLogFormatter,self).__init__(kwargs['format'])
# 
#     def format(self, record):
# #         record.tenant = tenant_holder.name
# #         record.trace = tracer.trace
# #         if tenant_holder.login_tenant_name != record.tenant:
# #             record.username = "%s@%s" % (tenant_holder.user_name, tenant_holder.login_tenant_name)
# #         else:
# #             record.username = tenant_holder.user_name
#         return Formatter.format(self, record)
