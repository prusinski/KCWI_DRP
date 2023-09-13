"""
KCWI

@author: lrizzi
"""

from keckdrpframework.pipelines.base_pipeline import BasePipeline
from keckdrpframework.models.processing_context import ProcessingContext
from kcwidrp.primitives.kcwi_file_primitives import *
from kcwidrp.core.kcwi_proctab import Proctab


class Kcwi_pipeline(BasePipeline):
    """
    Pipeline to process KCWI data

    """
    name = 'KCWI-DRP'

    event_table = {
        # this method is used with the "group" option,
        # to ingest the data without triggering any processing.
        # it is defined lower in this file
        "add_only":                  ("add_to_dataframe_only", None, None),
        #
        "start_bokeh":               ("StartBokeh", None, None),
        # For every file do this
        "next_file":                 ("ingest_file",
                                      "ingest_file_started",
                                      "file_ingested"),
        "file_ingested":             ("action_planner", None, None),

        # OBJECT PROCESSING
        "process_object":            ("ProcessObject",
                                      "object_processing_started",
                                      "object_wavelengthcorr"),
        "object_wavelengthcorr":     ("WavelengthCorrections",
                                      "wavelength_correction_started",  # icubew
                                      "object_correct_dar"),
        "object_correct_dar":        ("CorrectDar",
                                      "correcting_dar_started",     # icubed
                                      "object_make_invsens"),
        "object_make_invsens":       ("MakeInvsens",
                                      "make_invsens_started",
                                      "object_flux_calibrate"),
        "object_flux_calibrate":     ("FluxCalibrate",
                                      "flux_calibration_started",   # icubes
                                      None),
        "next_file_stop":            ("ingest_file", "file_ingested", None)
    }

    # event_table = kcwi_event_table

    def __init__(self, context: ProcessingContext):
        """
        Constructor
        """
        BasePipeline.__init__(self, context)
        self.cnt = 0

    def add_to_dataframe_only(self, action, context):
        return action.args

    def action_planner(self, action, context):
        try:
            self.context.pipeline_logger.info(
                "******* FILE TYPE DETERMINED AS %s" % action.args.imtype)
        except:
            self.context.pipeline_logger.warn(
                "******* FILE TYPE is NOT determined. "
                "No processing is possible.")
            return False

        groupid = action.args.groupid
        camera = action.args.ccddata.header['CAMERA'].upper()
        self.context.pipeline_logger.info("******* GROUPID is %s " %
                                          action.args.groupid)
        self.context.pipeline_logger.info(
            "******* STATEID is %s (%s) " %
            (action.args.ccddata.header["STATENAM"],
             action.args.ccddata.header["STATEID"]))
        self.context.pipeline_logger.info("******* CAMERA is %s " % camera)
        if action.args.in_proctab:
            self.context.pipeline_logger.warn(
                "Already processed (already in proctab)")
        if action.args.in_proctab and not context.config.instrument.clobber:
            self.context.pipeline_logger.warn("Pushing noop to queue")
            context.push_event("noop", action.args)
        elif "BIAS" in action.args.imtype:
            if action.args.ttime > 0:
                self.context.pipeline_logger.warn(
                    f"BIAS frame with exposure time = {action.args.ttime} "
                    f"> 0. Discarding.")
                return False
            bias_args = action.args
            bias_args.groupid = groupid
            bias_args.want_type = "BIAS"
            bias_args.new_type = "MBIAS"
            bias_args.min_files = context.config.instrument.bias_min_nframes
            bias_args.new_file_name = "master_bias_%s.fits" % groupid
            context.push_event("process_bias", bias_args)
        elif "DARK" in action.args.imtype:
            dark_args = action.args
            dark_args.groupid = groupid
            dark_args.want_type = "DARK"
            dark_args.new_type = "MDARK"
            dark_args.min_files = context.config.instrument.dark_min_nframes
            dark_args.new_file_name = "master_dark_%s.fits" % groupid
            dark_args.in_directory = "redux"
            context.push_event("process_dark", dark_args)
        elif "CONTBARS" in action.args.imtype:
            contbars_args = action.args
            contbars_args.groupid = groupid
            contbars_args.want_type = "CONTBARS"
            contbars_args.new_type = "MCBARS"
            contbars_args.min_files = int(context.config.instrument[camera][
                                              'contbars_min_nframes'])
            contbars_args.new_file_name = "master_contbars_%s.fits" % groupid
            contbars_args.in_directory = "redux"
            context.push_event("process_contbars", contbars_args)
        elif "FLATLAMP" in action.args.imtype:
            flat_args = action.args
            flat_args.groupid = groupid
            flat_args.want_type = "FLATLAMP"
            flat_args.stack_type = "SFLAT"
            flat_args.new_type = "MFLAT"
            flat_args.min_files = context.config.instrument.flat_min_nframes
            flat_args.new_file_name = "master_flat_%s.fits" % groupid
            flat_args.in_directory = "redux"
            context.push_event("process_flat", flat_args)
        elif "DOMEFLAT" in action.args.imtype:
            flat_args = action.args
            flat_args.groupid = groupid
            flat_args.want_type = "DOMEFLAT"
            flat_args.stack_type = "SDOME"
            flat_args.new_type = "MDOME"
            flat_args.min_files = context.config.instrument.dome_min_nframes
            flat_args.new_file_name = "master_flat_%s.fits" % groupid
            flat_args.in_directory = "redux"
            context.push_event("process_flat", flat_args)
        elif "TWIFLAT" in action.args.imtype:
            flat_args = action.args
            flat_args.groupid = groupid
            flat_args.want_type = "TWIFLAT"
            flat_args.stack_type = "STWIF"
            flat_args.new_type = "MTWIF"
            flat_args.min_files = context.config.instrument.twiflat_min_nframes
            flat_args.new_file_name = "master_flat_%s.fits" % groupid
            flat_args.in_directory = "redux"
            context.push_event("process_flat", flat_args)
        elif "ARCLAMP" in action.args.imtype:
            arc_args = action.args
            arc_args.groupid = groupid
            arc_args.want_type = "ARCLAMP"
            arc_args.new_type = "MARC"
            arc_args.min_files = int(context.config.instrument[camera][
                                         'arc_min_nframes'])
            arc_args.new_file_name = "master_arc_%s.fits" % groupid
            arc_args.in_directory = "redux"
            context.push_event("process_arc", arc_args)
        elif "OBJECT" in action.args.imtype:
            if action.args.nasmask and action.args.numopen > 1:
                context.push_event("process_nandshuff", action.args)
            else:
                object_args = action.args
                # object_args.new_type = "SKY"
                object_args.new_type = "MOBJ"
                object_args.min_files = int(
                    context.config.instrument[camera]['object_min_nframes'])
                object_args.in_directory = "redux"
                context.push_event("process_object", object_args)
        return True


if __name__ == "__main__":
    """
    Standalone test
    """
    pass
