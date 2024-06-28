WORK_ROOT=../../../


all:
	$(WORK_ROOT)/devel/ymake/ymake.sh t=...


opt:
	$(WORK_ROOT)/devel/ymake/ymake.sh t=... c=opt
#  if test $$? -ne 0; then exit 1; fi; \
#  cp $(WORK_ROOT)/.ymake-out/opt/$(PROJ_NAME)/*** $(WORK_ROOT)/$(PROJ_NAME)/; \

cp:
	cp $(WORK_ROOT)/.ymake-out/dbg/storage/rafdb/client/test_client .

clean:
	#rm -rf $(WORK_ROOT)/$(PROJ_NAME)/* \
  rm -rf $(WORK_ROOT)/.ymake-out/dbg/$(PROJ_NAME)/* \
  rm -rf $(WORK_ROOT)/.ymake-out/opt/$(PROJ_NAME)/* \
