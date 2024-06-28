WORK_ROOT=../../../
PROJ_NAME=storage/rafdb/proto


all:
	$(WORK_ROOT)/devel/ymake/ymake.sh t=...
#  if test $$? -ne 0; then exit 1; fi; \
#  cp $(WORK_ROOT)/.ymake-out/dbg/$(PROJ_NAME)/*** $(WORK_ROOT)/$(PROJ_NAME)/; \


opt:
	$(WORK_ROOT)/devel/ymake/ymake.sh t=... c=opt
#  if test $$? -ne 0; then exit 1; fi; \
#  cp $(WORK_ROOT)/.ymake-out/opt/$(PROJ_NAME)/*** $(WORK_ROOT)/$(PROJ_NAME)/; \


clean:
	#rm -rf $(WORK_ROOT)/$(PROJ_NAME)/* \
  rm -rf $(WORK_ROOT)/.ymake-out/dbg/$(PROJ_NAME)/* \
  rm -rf $(WORK_ROOT)/.ymake-out/opt/$(PROJ_NAME)/* \
