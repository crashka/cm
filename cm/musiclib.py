# -*- coding: utf-8 -*-

"""Music Library module
"""

import regex as re

from .utils import LOV, str_similarity
from .core import log
from .musicent import clean_user_keys, get_entity, key_data, entity_data, ml_dict
from .musicent import cond_role, IntegrityError, StringCtx, ParseFlag

#######################
# constants/functions #
#######################

# Lists of Values
NameVal = LOV({'NONE'   : '<none>',
               'UNKNOWN': '<unknown>'})

def insert_program_play(playlist, data):
    """
    :param playlist: parent Playlist object
    :param data: normalized playlist key/value data (dict)
    :return: key-value dict comprehension for inserted program_play fields
    """
    station = playlist.station
    # TODO: get rid of this hardwired structure (implicitly use fields from config file)!!!
    sta_data = {'name'      : station.name,
                'timezone'  : station.timezone,
                'synd_level': station.synd_level}
    sta = get_entity('station')
    sel_res = sta.select(key_data(sta_data, 'station'))
    if sel_res.rowcount == 1:
        sta_row = sel_res.fetchone()
    else:
        log.trace("Inserting station \"%s\" into musiclib" % station.name)
        ins_res = sta.insert(sta_data)
        if ins_res.rowcount == 0:
            raise RuntimeError("Could not insert station \"%s\" into musiclib" % station.name)
        sta_row = sta.inserted_row(ins_res)
        if not sta_row:
            raise RuntimeError("Station %s not in musiclib" % station.name)

    prog_data = data['program']
    prog = get_entity('program')
    sel_res = prog.select(key_data(prog_data, 'program'))
    if sel_res.rowcount == 1:
        prog_row = sel_res.fetchone()
    else:
        prog_name = prog_data['name']  # for convenience
        prog_label = "\"%s\"" % (prog_name)
        log.trace("Inserting program %s into musiclib" % prog_label)
        ins_res = prog.insert(prog_data)
        if ins_res.rowcount == 0:
            raise RuntimeError("Could not insert program %s into musiclib" % prog_label)
        prog_row = prog.inserted_row(ins_res)
        if not prog_row:
            raise RuntimeError("Program %s not in musiclib" % prog_label)

    pp_row = None
    pp_data = data['program_play']
    pp_data['station_id'] = sta_row.id
    pp_data['program_id'] = prog_row.id
    prog_play = get_entity('program_play')
    try:
        ins_res = prog_play.insert(pp_data)
        pp_row = prog_play.inserted_row(ins_res)
        log.trace("Created program_play ID %d (%s, \"%s\", %s %s)" %
                  (pp_row.id, sta_row.name, prog_row.name,
                   pp_row.prog_play_date, pp_row.prog_play_start))
    except IntegrityError:
        # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
        sel_res = prog_play.select(key_data(pp_data, 'program_play'))
        if sel_res.rowcount == 1:
            pp_row = sel_res.fetchone()
            log.debug("Skipping insert of duplicate program_play record (ID %d)" % pp_row.id)
        else:
            pass  # REVISIT: is this an internal error???
    return {k: v for k, v in pp_row.items()} if pp_row else None

def insert_play(playlist, prog_play, data):
    """
    :param playlist: parent Playlist object
    :param prog_play: parent program_play fields (dict)
    :param data: normalized play key/value data (dict)
    :return: key-value dict comprehension for inserted play fields
    """
    station = playlist.station
    # TODO: see above, insert_program_play()!!!  In addition, note that we should
    # really not have to requery here--the right thing to do is cache the station
    # record (in fact, we really only need the id here)!!!
    sta_data = {'name'      : station.name,
                'timezone'  : station.timezone,
                'synd_level': station.synd_level}
    sta = get_entity('station')
    sel_res = sta.select(key_data(sta_data, 'station'))
    if sel_res.rowcount == 1:
        sta_row = sel_res.fetchone()
    else:
        # NOTE: should really never get here (select should not fail), since this
        # same code was executed when inserting the program_play
        log.trace("Inserting station \"%s\" into musiclib" % station.name)
        ins_res = sta.insert(sta_data)
        if ins_res.rowcount == 0:
            raise RuntimeError("Could not insert station \"%s\" into musiclib" % station.name)
        sta_row = sta.inserted_row(ins_res)
        if not sta_row:
            raise RuntimeError("Station %s not in musiclib" % station.name)

    comp_data = data['composer']
    # NOTE: we always make sure there is a composer record (even if NONE or UNKNOWN), since work depends
    # on it (and there is no play without work, haha)
    if not comp_data.get('name'):
        comp_data['name'] = NameVal.NONE
    comp = get_entity('person')
    sel_res = comp.select(key_data(comp_data, 'person'))
    if sel_res.rowcount == 1:
        comp_row = sel_res.fetchone()
        if not comp_row.is_composer:
            comp.update(comp_row, {'is_composer': True})
    else:
        comp_name = comp_data['name']  # for convenience
        log.trace("Inserting composer \"%s\" into musiclib" % comp_name)
        ins_res = comp.insert(comp_data)
        if ins_res.rowcount == 0:
            raise RuntimeError("Could not insert composer/person \"%s\" into musiclib" % comp_name)
        comp_row = comp.inserted_row(ins_res)
        if not comp_row:
            raise RuntimeError("Composer/person \"%s\" not in musiclib" % comp_name)

    work_data = data['work']
    if not work_data.get('name'):
        # REVISIT: for how, insert '<unknown>' work, just so we have a record of this and can
        # try and identify the scenario (note, should really be logging to exception table)!!!
        work_data['name'] = NameVal.UNKNOWN
        #log.debug("Work name not specified, skipping...")
        #return None
    work_data['composer_id'] = comp_row.id
    work = get_entity('work')
    sel_res = work.select(key_data(work_data, 'work'))
    if sel_res.rowcount == 1:
        work_row = sel_res.fetchone()
    else:
        work_name = work_data['name']  # for convenience
        log.trace("Inserting work \"%s\" into musiclib" % work_name)
        ins_res = work.insert(work_data)
        if ins_res.rowcount == 0:
            raise RuntimeError("Could not insert work/person \"%s\" into musiclib" % work_name)
        work_row = work.inserted_row(ins_res)
        if not work_row:
            raise RuntimeError("Work/person \"%s\" not in musiclib" % work_name)

    cond_row = None
    cond_data = data['conductor']
    if cond_data.get('name'):
        cond = get_entity('person')
        sel_res = cond.select(key_data(cond_data, 'person'))
        if sel_res.rowcount == 1:
            cond_row = sel_res.fetchone()
            if not cond_row.is_conductor:
                cond.update(cond_row, {'is_conductor': True})
        else:
            cond_name = cond_data['name']  # for convenience
            log.trace("Inserting conductor \"%s\" into musiclib" % cond_name)
            ins_res = cond.insert(cond_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert conductor/person \"%s\" into musiclib" % cond_name)
            cond_row = cond.inserted_row(ins_res)
            if not cond_row:
                raise RuntimeError("Conductor/person \"%s\" not in musiclib" % cond_name)

    rec_row = None
    rec_data = data['recording']
    clean_user_keys(rec_data, 'recording')
    clean_user_keys(rec_data, 'recording_alt')
    if rec_data.get('label') and rec_data.get('catalog_no'):
        rec = get_entity('recording')
        sel_res = rec.select(key_data(rec_data, 'recording'))
        if sel_res.rowcount == 1:
            rec_row = sel_res.fetchone()
        else:
            rec_ident = "%s %s" % (rec_data['label'], rec_data['catalog_no'])  # for convenience
            log.trace("Inserting recording \"%s\" into musiclib" % rec_ident)
            ins_res = rec.insert(rec_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert recording \"%s\" into musiclib" % rec_ident)
            rec_row = rec.inserted_row(ins_res)
            if not rec_row:
                raise RuntimeError("Recording \"%s\" not in musiclib" % rec_ident)
    elif rec_data.get('name'):
        rec = get_entity('recording')
        sel_res = rec.select(key_data(rec_data, 'recording_alt'))
        if sel_res.rowcount == 1:
            rec_row = sel_res.fetchone()
        elif sel_res.rowcount > 1:
            # REVISIT: just pick the first one randomly???
            rec_row = sel_res.fetchone()
        else:
            rec_name = rec_data['name']  # for convenience
            log.trace("Inserting recording \"%s\" into musiclib" % rec_name)
            ins_res = rec.insert(rec_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert recording \"%s\" into musiclib" % rec_name)
            rec_row = rec.inserted_row(ins_res, 'recording_alt')
            if not rec_row:
                raise RuntimeError("Recording \"%s\" not in musiclib" % rec_name)

    perf_rows = []
    for perf_data in data['performers']:
        # STEP 1 -: insert/select underlying person record
        perf_person = get_entity('person')  # cached, so okay to re-get for each loop
        sel_res = perf_person.select(key_data(perf_data['person'], 'person'))
        if sel_res.rowcount == 1:
            perf_person_row = sel_res.fetchone()
            if not cond_role(perf_data['role']) and not perf_person_row.is_performer:
                perf_person.update(perf_person_row, {'is_performer': True})
        else:
            perf_name = perf_data['person']['name']  # for convenience
            log.trace("Inserting performer/person \"%s\" into musiclib" % perf_name)
            ins_res = perf_person.insert(perf_data['person'])
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert performer/person \"%s\" into musiclib" % perf_name)
            perf_person_row = perf_person.inserted_row(ins_res)
            if not perf_person_row:
                raise RuntimeError("Performer/person \"%s\" not in musiclib" % perf_name)
        perf_data['person_id'] = perf_person_row.id

        # STEP 2 - now deal with performer record (since we have the person)
        perf = get_entity('performer')  # cached, so okay to re-get for each loop
        sel_res = perf.select(key_data(perf_data, 'performer'))
        if sel_res.rowcount == 1:
            perf_row = sel_res.fetchone()
        else:
            perf_name = perf_data['person']['name']  # for convenience
            perf_role = perf_data['role']
            perf_label = "\"%s\" [%s]" % (perf_name, perf_role)
            log.trace("Inserting performer %s into musiclib" % perf_label)
            ins_res = perf.insert(entity_data(perf_data, 'performer'))
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert performer %s into musiclib" % perf_label)
            perf_row = perf.inserted_row(ins_res)
            if not perf_row:
                raise RuntimeError("Performer %s not in musiclib" % perf_label)
        perf_rows.append(perf_row)

    ens_rows = []
    for ens_data in data['ensembles']:
        ens = get_entity('ensemble')  # cached, so okay to re-get for each loop
        sel_res = ens.select(key_data(ens_data, 'ensemble'))
        if sel_res.rowcount == 1:
            ens_row = sel_res.fetchone()
        else:
            ens_name = ens_data['name']  # for convenience
            log.trace("Inserting ensemble \"%s\" into musiclib" % ens_name)
            ins_res = ens.insert(ens_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert ensemble \"%s\" into musiclib" % ens_name)
            ens_row = ens.inserted_row(ins_res)
            if not ens_row:
                raise RuntimeError("Ensemble \"%s\" not in musiclib" % ens_name)
        ens_rows.append(ens_row)

    play_new = False
    play_row = None
    play_data = data['play']
    play_data['station_id']   = sta_row.id
    play_data['prog_play_id'] = prog_play['id']
    play_data['program_id']   = prog_play['program_id']
    play_data['composer_id']  = comp_row.id
    play_data['work_id']      = work_row.id
    if cond_row:
        play_data['conductor_id'] = cond_row.id
    # NOTE: performer_ids and ensemble_ids are denorms, with no integrity checking
    if perf_rows:
        play_data['performer_ids'] = [perf_row.id for perf_row in perf_rows]
    if ens_rows:
        play_data['ensemble_ids'] = [ens_row.id for ens_row in ens_rows]
    play = get_entity('play')
    try:
        ins_res = play.insert(play_data)
        play_row = play.inserted_row(ins_res)
        play_new = True
        log.trace("Created play ID %d (%s, \"%s\", %s %s)" %
                  (play_row.id, comp_row.name, work_row.name,
                   play_row.play_date, play_row.play_start))
    except IntegrityError:
        # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
        log.debug("Skipping insert of duplicate play record:\n%s" % play_data)
        sel_res = play.select(key_data(play_data, 'play'))
        if sel_res.rowcount == 1:
            play_row = sel_res.fetchone()
        else:
            pass  # REVISIT: is this an internal error???

    # write intersect records that are authoritative (denormed as arrays of keys, above)
    play_perf_rows = []
    play_ens_rows = []
    if play_new:
        for perf_row in perf_rows:
            play_perf_data = {'play_id': play_row.id, 'performer_id': perf_row.id}
            play_perf = get_entity('play_performer')
            try:
                ins_res = play_perf.insert(play_perf_data)
                play_perf_rows.append(play_perf.inserted_row(ins_res))
            except IntegrityError:
                log.trace("Skipping insert of duplicate play_performer record:\n%s" % play_perf_data)

        for ens_row in ens_rows:
            play_ens_data = {'play_id': play_row.id, 'ensemble_id': ens_row.id}
            play_ens = get_entity('play_ensemble')
            try:
                ins_res = play_ens.insert(play_ens_data)
                play_ens_rows.append(play_ens.inserted_row(ins_res))
            except IntegrityError:
                log.trace("Skipping insert of duplicate play_ensemble record:\n%s" % play_ens_data)

    return {k: v for k, v in play_row.items()}

def insert_play_seq(play_rec, play_seq, hash_type):
    """
    :param play_rec:
    :param play_seq:
    :param hash_type:
    :return: list of key-value dict comprehensions for inserted play_seq fields
    """
    ret = []
    ps = get_entity('play_seq')
    while play_seq:
        level = len(play_seq)
        hashval = play_seq.pop(0)
        data = {
            'hash_level': level,
            'hash_type' : hash_type,
            'play_id'   : play_rec['id'],
            'seq_hash'  : hashval
        }

        try:
            ins_res = ps.insert(data)
            ps_row = ps.inserted_row(ins_res)
            ret.append({k: v for k, v in ps_row.items()})
        except IntegrityError:
            log.debug("Could not insert play_seq %s into musiclib" % data)

    return ret

def insert_entity_strings(playlist, data):
    """
    :return: list of key-value dict comprehensions for inserted entity_string fields
    """
    ctx = playlist.parse_ctx
    ret = []
    es = get_entity('entity_string')
    for entity_src, src_strings in data.items():
        for entity_str in src_strings:
            if not (entity_str and re.search(r'\w', entity_str)):
                continue
            ent_str_data = {
                'entity_str'  : entity_str,
                'source_fld'  : entity_src,
                'station_id'  : ctx['station_id'],
                'prog_play_id': ctx['prog_play_id'],
                'play_id'     : ctx['play_id']
            }

            try:
                ins_res = es.insert(ent_str_data)
                es_row = es.inserted_row(ins_res)
                ret.append({k: v for k, v in es_row.items()})
            except IntegrityError:
                log.trace("Duplicate entity_string \"%s\" [%s] for station ID %d" %
                          (entity_str, entity_src, ctx['station_id']))

    return ret

def insert_entity_ref(refdata, ent_data, ent_refs, raw_name = None):
    """
    :return: key-value dict comprehension for inserted entity_ref fields
    """
    #ctx = refdata.parse_ctx
    ent_name = ent_data['entity_ref']
    ent_type = ent_data['entity_type']
    ref_source = ent_data['ref_source']
    ret = []
    er = get_entity('entity_ref')

    try:
        ins_res = er.insert(ent_data)
        es_row = er.inserted_row(ins_res)
        ret.append({k: v for k, v in es_row.items()})
    except IntegrityError:
        log.trace("Duplicate entity name \"%s\" [%s] for refdata \"%s\"" %
                  (ent_name, ent_type, ref_source))

    ent_ref_data = ent_data.copy()
    del ent_ref_data['is_entity']
    for ref_str in ent_refs:
        ent_ref_data['entity_ref'] = ref_str,
        ent_ref_data['ref_strength'] = round(str_similarity(ref_str, ent_name) * 100.0)
        # FIX: see comment in caller (refdata.py)!!!
        if raw_name and ref_str == raw_name:
            ent_ref_data['is_raw'] = True
        elif ent_ref_data.get('is_raw'):
            del ent_ref_data['is_raw']

        try:
            ins_res = er.insert(ent_ref_data)
            es_row = er.inserted_row(ins_res)
            ret.append({k: v for k, v in es_row.items()})
        except IntegrityError:
            log.trace("Duplicate entity_ref \"%s\" [%s] for refdata \"%s\"" %
                      (ref_str, ent_type, ref_source))

    return ret

def parse_composer_str(comp_str, flags = 0):
    """
    :param comp_str: raw string from playlist
    :param flags: [int/bitfield] later
    :return: ml_dict of parsed data
    """
    if not comp_str or not re.search(r'\w', comp_str):
        return {}

    ctx = StringCtx(comp_str, flags | ParseFlag.COMPOSER)
    ctx.parse_entity_str()

    comp_data = ctx.mkcomp(ctx.ent_str)
    ent_data = ml_dict({'composer': comp_data})
    ctx.finalize(ent_data)
    return ent_data

def parse_work_str(work_str, flags = 0):
    """
    :param work_str: raw string from playlist
    :param flags: [int/bitfield] later
    :return: ml_dict of parsed data
    """
    if not work_str or not re.search(r'\w', work_str):
        return {}

    ctx = StringCtx(work_str, flags | ParseFlag.WORK)
    ctx.parse_entity_str()

    work_data = ctx.mkwork(ctx.ent_str)
    ent_data = ml_dict({'work': work_data})
    ctx.finalize(ent_data)
    return ent_data

def parse_conductor_str(cond_str, flags = 0):
    """
    :param cond_str: raw string from playlist
    :param flags: [int/bitfield] later
    :return: ml_dict of parsed data
    """
    if not cond_str or not re.search(r'\w', cond_str):
        return {}

    ctx = StringCtx(cond_str, flags | ParseFlag.CONDUCTOR)
    ctx.parse_entity_str()

    cond_data = ctx.mkcond(ctx.ent_str)
    perf_data = [ctx.mkperf(ctx.ent_str, 'conductor')]
    ent_data = ml_dict({'conductor': cond_data, 'performers': perf_data})
    ctx.finalize(ent_data)
    return ent_data

def parse_performer_str(perf_str, flags = 0):
    """
    DESIGN NOTES (for future):
      * context-sensitive application of individual parsing rules, either implicitly
        (e.g. based on station), or explicitly through flags
      * generic parsing using non-alphanum delimiters, entity lookups (refdata), and
        logical entity relationships (either as replacement, or complement)
      * for now, we return performer data only; LATER: need the ability to indicate
        other entities extracted from perf_str!!!

    :param perf_str:
    :param flags: (not yet implemented)
    :return: list of perf_data structures (see LATER above)
    """
    if not perf_str or not re.search(r'\w', perf_str):
        return {}

    ctx = StringCtx(perf_str, flags | ParseFlag.PERFORMER)
    ctx.parse_entity_str()

    def parse_perf_item(perf_item, fld_delim = ','):
        sub_perfs = []
        sub_cond = None
        if perf_item.count(fld_delim) % 2 == 1:
            fields = perf_item.split(fld_delim)
            while fields:
                pers, role = (fields.pop(0), fields.pop(0))
                # special case for "<ens>/<cond last, first>"
                if pers.count('/') == 1:
                    log.debug("PFS_RULE 6 - slash separating ens from cond_last \"%s\"" % pers)
                    ens_name, cond_last = pers.split('/')
                    cond_name = "%s %s" % (role, cond_last)
                    sub_perfs.append(ctx.mkperf(ens_name, 'ensemble'))
                    sub_perfs.append(ctx.mkperf(cond_name, 'conductor'))
                else:
                    if cond_role(role):
                        # TODO: check for overwrite!!!
                        sub_cond = ctx.mkcond(pers)
                    sub_perfs.append(ctx.mkperf(pers, role))
        else:
            # TODO: if even number of field delimiters, need to look closer at item
            # contents/format to figure out what to do!!!
            sub_perfs.append(ctx.mkperf(perf_item, None))
        return {'performers': sub_perfs, 'conductor': sub_cond} if sub_cond \
               else {'performers': sub_perfs}

    ens_data  = []
    perf_data = []
    ret_data  = ml_dict({'ensembles': ens_data, 'performers': perf_data})
    """
    # TODO: should really move the quote processing as far upstream as possible (for
    # all fields); NOTE: also need to revisit normalize_* functions in musiclib!!!
    m = re.fullmatch(r'"([^"]*)"', perf_str)
    if m:
        log.debug("PFS_RULE 1 - strip enclosing quotes \"%s\"" % (perf_str))
        perf_str = m.group(1)  # note: could be empty string, handle downstream!
    m = re.fullmatch(r'\((.*[^)])\)?', perf_str)
    if m:
        log.debug("PFS_RULE 2 - strip enclosing parens \"%s\"" % (perf_str))
        perf_str = m.group(1)  # note: could be empty string, handle downstream!
    """
    # TODO: genericize performer/person/role stuff (note, ctx.ent_str not updated below)!!!
    perf_str = ctx.ent_str

    # special case for ugly record (WNED 2018-09-17)
    m = re.match(r'(.+?)\r', perf_str)
    if m:
        log.debug("PFS_RULE 3 - ugly broken record for WNED \"%s\"" % perf_str)
        perf_str = m.group(1)
        m = re.match(r'(.+)\[(.+)\],(.+)', perf_str)
        if m:
            perf_str = '; '.join(m.groups())

    # pattern used by IPR, VPR, WIAA, WNED
    if re.match(r'\/.+ \- ', perf_str):
        log.debug("PFS_RULE 4 - leading slash for performer fields \"%s\"" % perf_str)
        for perf_item in perf_str.split('/'):
            if perf_item:
                ret_data.merge(parse_perf_item(perf_item, ' - '))
    elif ';' in perf_str:
        log.debug("PFS_RULE 5 - semi-colon-deliminted performer fields \"%s\"" % perf_str)
        for perf_item in perf_str.split(';'):
            if perf_item:
                ret_data.merge(parse_perf_item(perf_item))
    elif perf_str:
        ret_data.merge(parse_perf_item(perf_str))

    ctx.finalize(ret_data)
    return ret_data

def parse_ensemble_str(ens_str, flags = 0):
    """
    :param ens_str:
    :param flags: (not yet implemented)
    :return: dict of ens_data/perf_data structures, indexed by type
    """
    if not ens_str or not re.search(r'\w', ens_str):
        return {}

    ctx = StringCtx(ens_str, flags | ParseFlag.ENSEMBLE)
    ctx.parse_entity_str()

    def parse_ens_item(ens_item, fld_delim = ','):
        sub_ens_data = []
        sub_perf_data = []
        if ens_item.count(fld_delim) % 2 == 1:
            fields = ens_item.split(fld_delim)
            while fields:
                name, role = (fields.pop(0), fields.pop(0))
                # TEMP: if role starts with a capital letter, assume the whole string
                # is an ensemble (though in reality, it may be two--we'll deal with
                # that later, when we have NER), otherwise treat as performer/role!!!
                #if re.match(r'[A-Z]', role[0]):
                if re.match(r'\p{Lu}', role[0]):
                    sub_ens_data.append(ctx.mkens(name))
                else:
                    sub_perf_data.append(ctx.mkperf(name, role))
        else:
            # TODO: if even number of field delimiters, need to look closer at item
            # contents/format to figure out what to do (i.e. NER)!!!
            sub_ens_data.append(ctx.mkens(ens_item))
        return {'ensembles' : sub_ens_data, 'performers': sub_perf_data}

    def parse_ens_fields(fields):
        sub_ens_data = []
        sub_perf_data = []
        while fields:
            if len(fields) == 1:
                sub_ens_data.append(ctx.mkens(fields.pop(0)))
                break  # same as continue
            # more reliable to do this moving backward from the end (sez me)
            if ' ' not in fields[-1]:
                # REVISIT: we presume a single-word field to be a city/location (for now);
                # as above, we should really look at field contents to properly parse!!!
                ens = ','.join([fields.pop(-2), fields.pop(-1)])
                sub_ens_data.append(ctx.mkens(ens))
            else:
                # yes, do this twice!
                sub_ens_data.append(ctx.mkens(fields.pop(-1)))
                sub_ens_data.append(ctx.mkens(fields.pop(-1)))
        return {'ensembles' : sub_ens_data, 'performers': sub_perf_data}

    ens_data  = []
    perf_data = []
    ret_data  = ml_dict({'ensembles': ens_data, 'performers': perf_data})
    if ';' in ens_str:
        for ens_item in ens_str.split(';'):
            if ens_item:
                ret_data.merge(parse_ens_item(ens_item))
    elif ',' in ens_str:
        ens_fields = ens_str.split(',')
        ret_data.merge(parse_ens_fields(ens_fields))
    else:
        # ens_data is implcitly part of ret_data
        ens_data.append(ctx.mkens(ens_str))

    return ret_data

#####################
# command line tool #
#####################

import sys

if __name__ == '__main__':
    if len(sys.argv) < 4:
        raise RuntimeError("Usage: musiclib.py [select|insert] <entity> <key>=<value> ...")

    ent = get_entity(sys.argv[2])
    meth = getattr(ent, sys.argv[1])
    data = {}
    for cond in sys.argv[3:]:
        (key, val) = cond.split('=')
        data[key] = int(val) if val.isdigit() else val
    res = meth(data)
    print("Rowcount: %d" % res.rowcount)
    if res.returns_rows:
        print(res.fetchall())
    else:
        print(res.__dict__)
